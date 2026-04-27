#include <stdio.h>
#include <stdbool.h>
#include <stdint.h>
#include <math.h>
#include <string.h>
#include <errno.h>

#include "esp_err.h"
#include <sdkconfig.h>
#include "esp_event.h"
#include "esp_log.h"
#include "esp_netif.h"
#include "esp_system.h"
#include "esp_wifi.h"
#include "nvs_flash.h"
#include "freertos/event_groups.h"
#include "freertos/FreeRTOS.h"
#include "freertos/task.h"
#include "freertos/queue.h"
#include <lwip/sockets.h>
#include <lwip/inet.h>

typedef enum {
	PARKING_STATE_SAFE = 0,
	PARKING_STATE_WARNING,
	PARKING_STATE_DANGER,
	PARKING_STATE_FAULT,
} parking_state_t;

typedef struct {
	uint32_t sequence;
	float raw_distance_cm;
	float filtered_distance_cm;
	bool fault;
	parking_state_t state;
} pipeline_sample_t;

static QueueHandle_t s_sensor_queue;
static QueueHandle_t s_filter_queue;
static QueueHandle_t s_validation_queue;
static QueueHandle_t s_decision_queue;
static QueueHandle_t s_feedback_queue;
static EventGroupHandle_t s_wifi_event_group;

static const char *const k_wifi_ssid = "Buchy";
static const char *const k_wifi_password = "userresu";
static const char *const k_wifi_tag = "wifi";
static const char *const k_host_ip = "10.36.57.133";
static const uint16_t k_host_port = 8888;

static const float k_operational_min_cm = 10.0f;
static const float k_operational_max_cm = 200.0f;
static const float k_safe_threshold_cm = 120.0f;
static const float k_warning_threshold_cm = 60.0f;

#define WIFI_CONNECTED_BIT BIT0
#define WIFI_FAIL_BIT BIT1

#define RAW_PACKET_MAGIC 0x52544553U

typedef struct __attribute__((packed)) {
	uint32_t magic_be;
	uint32_t sequence_be;
	uint32_t raw_distance_be;
	uint32_t filtered_distance_be;
	uint8_t fault;
	uint8_t state;
	uint16_t reserved_be;
} raw_telemetry_packet_t;

static uint32_t float_to_be(float value)
{
	uint32_t bits = 0;
	memcpy(&bits, &value, sizeof(bits));
	return htonl(bits);
}

static bool open_udp_socket(int *socket_fd, struct sockaddr_in *dest_addr)
{
	*socket_fd = socket(AF_INET, SOCK_DGRAM, IPPROTO_IP);
	if (*socket_fd < 0) {
		printf("[comm] socket creation failed: errno=%d\n", errno);
		return false;
	}

	memset(dest_addr, 0, sizeof(*dest_addr));
	dest_addr->sin_family = AF_INET;
	dest_addr->sin_port = htons(k_host_port);

	if (inet_pton(AF_INET, k_host_ip, &dest_addr->sin_addr) != 1) {
		printf("[comm] invalid host ip '%s'\n", k_host_ip);
		close(*socket_fd);
		*socket_fd = -1;
		return false;
	}

	printf("[comm] UDP telemetry target %s:%u\n", k_host_ip, (unsigned int)k_host_port);
	return true;
}

static void wifi_event_handler(void *arg, esp_event_base_t event_base, int32_t event_id, void *event_data)
{
	(void)arg;

	if (event_base == WIFI_EVENT && event_id == WIFI_EVENT_STA_START) {
		esp_wifi_connect();
	} else if (event_base == WIFI_EVENT && event_id == WIFI_EVENT_STA_DISCONNECTED) {
		xEventGroupClearBits(s_wifi_event_group, WIFI_CONNECTED_BIT);
		xEventGroupSetBits(s_wifi_event_group, WIFI_FAIL_BIT);
		ESP_LOGI(k_wifi_tag, "disconnected, retrying connection");
		esp_wifi_connect();
	} else if (event_base == IP_EVENT && event_id == IP_EVENT_STA_GOT_IP) {
		ip_event_got_ip_t *event = (ip_event_got_ip_t *)event_data;
		xEventGroupClearBits(s_wifi_event_group, WIFI_FAIL_BIT);
		xEventGroupSetBits(s_wifi_event_group, WIFI_CONNECTED_BIT);
		ESP_LOGI(k_wifi_tag, "got ip: " IPSTR, IP2STR(&event->ip_info.ip));
	}
}

static void wifi_connect(void)
{
	esp_err_t ret = nvs_flash_init();
	if (ret == ESP_ERR_NVS_NO_FREE_PAGES || ret == ESP_ERR_NVS_NEW_VERSION_FOUND) {
		ESP_ERROR_CHECK(nvs_flash_erase());
		ret = nvs_flash_init();
	}
	ESP_ERROR_CHECK(ret);

	ESP_ERROR_CHECK(esp_netif_init());
	ESP_ERROR_CHECK(esp_event_loop_create_default());
	esp_netif_create_default_wifi_sta();

	s_wifi_event_group = xEventGroupCreate();
	ESP_ERROR_CHECK(s_wifi_event_group != NULL ? ESP_OK : ESP_FAIL);

	wifi_init_config_t cfg = WIFI_INIT_CONFIG_DEFAULT();
	ESP_ERROR_CHECK(esp_wifi_init(&cfg));
	ESP_ERROR_CHECK(esp_event_handler_register(WIFI_EVENT, ESP_EVENT_ANY_ID, &wifi_event_handler, NULL));
	ESP_ERROR_CHECK(esp_event_handler_register(IP_EVENT, IP_EVENT_STA_GOT_IP, &wifi_event_handler, NULL));

	wifi_config_t wifi_config = {
		.sta = {
		},
	};
	strncpy((char *)wifi_config.sta.ssid, k_wifi_ssid, sizeof(wifi_config.sta.ssid));
	strncpy((char *)wifi_config.sta.password, k_wifi_password, sizeof(wifi_config.sta.password));

	ESP_ERROR_CHECK(esp_wifi_set_mode(WIFI_MODE_STA));
	ESP_ERROR_CHECK(esp_wifi_set_config(WIFI_IF_STA, &wifi_config));
	ESP_ERROR_CHECK(esp_wifi_start());

	ESP_LOGI(k_wifi_tag, "connecting to SSID=%s", k_wifi_ssid);

	EventBits_t bits = xEventGroupWaitBits(s_wifi_event_group,
							 WIFI_CONNECTED_BIT | WIFI_FAIL_BIT,
							 pdFALSE,
							 pdFALSE,
							 portMAX_DELAY);
	if (bits & WIFI_CONNECTED_BIT) {
		ESP_LOGI(k_wifi_tag, "connected to AP SSID:%s password:%s", k_wifi_ssid, k_wifi_password);
	} else {
		ESP_LOGI(k_wifi_tag, "failed to connect to SSID:%s password:%s", k_wifi_ssid, k_wifi_password);
	}
}

static const char *state_to_text(parking_state_t state)
{
	switch (state) {
	case PARKING_STATE_SAFE:
		return "SAFE";
	case PARKING_STATE_WARNING:
		return "WARNING";
	case PARKING_STATE_DANGER:
		return "DANGER";
	case PARKING_STATE_FAULT:
	default:
		return "FAULT";
	}
}

static float random_uniform(float min_value, float max_value)
{
	uint32_t r = esp_random();
	float unit = (float)r / (float)UINT32_MAX;
	return min_value + (unit * (max_value - min_value));
}

static float simulate_distance_cm(uint32_t sequence)
{
	const float center_cm = (k_operational_min_cm + k_operational_max_cm) * 0.5f;
	const float amplitude_cm = (k_operational_max_cm - k_operational_min_cm) * 0.5f;
	const float wave_period_samples = 80.0f;
	const float two_pi = 6.283185307f;

	float phase = two_pi * ((float)(sequence % (uint32_t)wave_period_samples) / wave_period_samples);
	float wave = center_cm + (amplitude_cm * sinf(phase));
	float noise = random_uniform(-4.0f, 4.0f);
	float measurement = wave + noise;

	if ((esp_random() % 40U) == 0U) {
		float spike = random_uniform(-80.0f, 80.0f);
		measurement += spike;
		printf("[sensor] spike injected: %.2f cm\n", spike);
	}

	return measurement;
}

static void sensor_task(void *arg)
{
	(void)arg;
	TickType_t last_wake_time = xTaskGetTickCount();
	uint32_t sequence = 0;

	while (true) {
		pipeline_sample_t sample = {0};
		sample.sequence = sequence++;
		sample.raw_distance_cm = simulate_distance_cm(sample.sequence);
		sample.filtered_distance_cm = sample.raw_distance_cm;

		printf("[sensor] #%lu raw=%.2f cm\n", (unsigned long)sample.sequence, sample.raw_distance_cm);
		xQueueSend(s_sensor_queue, &sample, portMAX_DELAY);

		vTaskDelayUntil(&last_wake_time, pdMS_TO_TICKS(250));
	}
}

static void filter_task(void *arg)
{
	(void)arg;
	pipeline_sample_t sample = {0};
	float previous_filtered = 0.0f;
	bool have_previous = false;

	while (true) {
		xQueueReceive(s_sensor_queue, &sample, portMAX_DELAY);

		if (have_previous) {
			sample.filtered_distance_cm = (0.60f * previous_filtered) + (0.4f * sample.raw_distance_cm);
		} else {
			sample.filtered_distance_cm = sample.raw_distance_cm;
			have_previous = true;
		}

		previous_filtered = sample.filtered_distance_cm;
		printf("[filter] #%lu filtered=%.2f cm\n", (unsigned long)sample.sequence, sample.filtered_distance_cm);
		xQueueSend(s_filter_queue, &sample, portMAX_DELAY);
	}
}

static void validation_task(void *arg)
{
	(void)arg;
	pipeline_sample_t sample = {0};

	while (true) {
		xQueueReceive(s_filter_queue, &sample, portMAX_DELAY);

		sample.fault = !isfinite(sample.raw_distance_cm) ||
					   (sample.raw_distance_cm < k_operational_min_cm) ||
					   (sample.raw_distance_cm > k_operational_max_cm);

		if (sample.fault) {
			sample.state = PARKING_STATE_FAULT;
			printf("[validate] out-of-range fault: %.2f cm (valid %.2f..%.2f)\n",
				   sample.raw_distance_cm,
				   k_operational_min_cm,
				   k_operational_max_cm);
		}

		printf("[validate] #%lu raw=%.2f cm fault=%s\n",
			   (unsigned long)sample.sequence,
			   sample.raw_distance_cm,
			   sample.fault ? "YES" : "NO");
		xQueueSend(s_validation_queue, &sample, portMAX_DELAY);
	}
}

static void decision_task(void *arg)
{
	(void)arg;
	pipeline_sample_t sample = {0};

	while (true) {
		xQueueReceive(s_validation_queue, &sample, portMAX_DELAY);

		if (!sample.fault) {
			if (sample.filtered_distance_cm < k_warning_threshold_cm) {
				sample.state = PARKING_STATE_DANGER;
			} else if (sample.filtered_distance_cm < k_safe_threshold_cm) {
				sample.state = PARKING_STATE_WARNING;
			} else {
				sample.state = PARKING_STATE_SAFE;
			}
		}

		printf("[decision] #%lu state=%s\n",
			   (unsigned long)sample.sequence,
			   state_to_text(sample.state));
		xQueueSend(s_decision_queue, &sample, portMAX_DELAY);
	}
}

static void feedback_task(void *arg)
{
	(void)arg;
	pipeline_sample_t sample = {0};

	while (true) {
		xQueueReceive(s_decision_queue, &sample, portMAX_DELAY);

		const char *feedback = "KEEP_MONITORING";
		switch (sample.state) {
		case PARKING_STATE_SAFE:
			feedback = "CLEAR_PATH";
			break;
		case PARKING_STATE_WARNING:
			feedback = "SLOW_DOWN";
			break;
		case PARKING_STATE_DANGER:
			feedback = "STOP_NOW";
			break;
		case PARKING_STATE_FAULT:
		default:
			feedback = "FAULT_REPORTED";
			break;
		}

		printf("[feedback] #%lu action=%s\n",
			   (unsigned long)sample.sequence,
			   feedback);
		xQueueSend(s_feedback_queue, &sample, portMAX_DELAY);
	}
}

static void comm_task(void *arg)
{
	(void)arg;
	pipeline_sample_t sample = {0};
	int socket_fd = -1;
	struct sockaddr_in dest_addr;
	raw_telemetry_packet_t packet;

	while (socket_fd < 0) {
		EventBits_t wifi_bits = xEventGroupGetBits(s_wifi_event_group);
		if ((wifi_bits & WIFI_CONNECTED_BIT) == 0U) {
			printf("[comm] waiting for Wi-Fi before UDP setup\n");
			vTaskDelay(pdMS_TO_TICKS(1000));
			continue;
		}

		if (!open_udp_socket(&socket_fd, &dest_addr)) {
			printf("[comm] UDP setup failed, retrying\n");
			vTaskDelay(pdMS_TO_TICKS(2000));
		}
	}

	while (true) {
		xQueueReceive(s_feedback_queue, &sample, portMAX_DELAY);

		packet.magic_be = htonl(RAW_PACKET_MAGIC);
		packet.sequence_be = htonl(sample.sequence);
		packet.raw_distance_be = float_to_be(sample.raw_distance_cm);
		packet.filtered_distance_be = float_to_be(sample.filtered_distance_cm);
		packet.fault = sample.fault ? 1U : 0U;
		packet.state = (uint8_t)sample.state;
		packet.reserved_be = htons(0U);

		ssize_t sent = sendto(
			socket_fd,
			&packet,
			sizeof(packet),
			0,
			(const struct sockaddr *)&dest_addr,
			sizeof(dest_addr));

		if (sent != (ssize_t)sizeof(packet)) {
			printf("[comm] UDP send failed: errno=%d, reconnecting socket\n", errno);
			close(socket_fd);
			socket_fd = -1;

			while (socket_fd < 0) {
				EventBits_t wifi_bits = xEventGroupGetBits(s_wifi_event_group);
				if ((wifi_bits & WIFI_CONNECTED_BIT) == 0U) {
					printf("[comm] Wi-Fi down while reconnecting UDP socket\n");
					vTaskDelay(pdMS_TO_TICKS(1000));
					continue;
				}

				if (!open_udp_socket(&socket_fd, &dest_addr)) {
					vTaskDelay(pdMS_TO_TICKS(2000));
				}
			}
			continue;
		}

		printf("[comm] #%lu sent raw=%.2f filtered=%.2f fault=%u state=%u\n",
			   (unsigned long)sample.sequence,
			   sample.raw_distance_cm,
			   sample.filtered_distance_cm,
			   (unsigned int)packet.fault,
			   (unsigned int)packet.state);
	}
}

void app_main(void)
{
	wifi_connect();

	s_sensor_queue = xQueueCreate(4, sizeof(pipeline_sample_t));
	s_filter_queue = xQueueCreate(4, sizeof(pipeline_sample_t));
	s_validation_queue = xQueueCreate(4, sizeof(pipeline_sample_t));
	s_decision_queue = xQueueCreate(4, sizeof(pipeline_sample_t));
	s_feedback_queue = xQueueCreate(4, sizeof(pipeline_sample_t));

	xTaskCreate(sensor_task, "sensor_task", 4096, NULL, 5, NULL);
	xTaskCreate(filter_task, "filter_task", 4096, NULL, 4, NULL);
	xTaskCreate(validation_task, "validation_task", 4096, NULL, 3, NULL);
	xTaskCreate(decision_task, "decision_task", 4096, NULL, 2, NULL);
	xTaskCreate(feedback_task, "feedback_task", 4096, NULL, 1, NULL);
	xTaskCreate(comm_task, "comm_task", 8192, NULL, 1, NULL);
}
