package uihandlers

import (
	"github.com/brian-nunez/scheduler-worker/internal/observability"
	"github.com/brian-nunez/scheduler-worker/views/pages"
	"github.com/labstack/echo/v4"
)

func HomeHandler(c echo.Context) error {
	observability.LogInfo(c.Request().Context(), "the home page loaded! 🫠")
	c.Response().Header().Set(echo.HeaderContentType, echo.MIMETextHTMLCharsetUTF8)

	return pages.Home().Render(c.Request().Context(), c.Response().Writer)
}
