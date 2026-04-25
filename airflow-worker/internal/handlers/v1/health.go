package v1

import (
	"net/http"

	"github.com/brian-nunez/airflow-worker/internal/observability"
	"github.com/labstack/echo/v4"
)

func HealthHandler(c echo.Context) error {
	observability.LogInfo(c.Request().Context(), "health_check: service is running")

	return c.JSON(http.StatusOK, map[string]string{
		"status":  "ok",
		"message": "Service is running",
		"TODO":    "UPDATE TO USE A REAL HEALTH CHECK",
	})
}
