document.addEventListener("DOMContentLoaded", () => {
    const guruScript = document.createElement("script");
    guruScript.src = "https://widget.gurubase.io/widget.latest.min.js";
    guruScript.defer = true;
    guruScript.id = "guru-widget-id";

    const widgetSettings = {
        // This token is binded to `faststream.airt.ai` domain,
        // so its publication is not a problem
        "data-widget-id": "TSM1_f2Ese6iwdKflPk7_ItbjXoRBi5CxL_BEYM2s_4",  // pragma: allowlist secrets
        "data-text": "Ask AI",
        "data-margins": JSON.stringify({ bottom: "20px", right: "20px" }),
        "data-light-mode": "false",
        "data-bg-color": "#003257",
        "data-icon-url": "https://faststream.airt.ai/latest/assets/img/logo.svg",
    };

    Object.entries(widgetSettings).forEach(([key, value]) => {
        guruScript.setAttribute(key, value);
    });

    document.body.appendChild(guruScript);
});
