document.addEventListener("DOMContentLoaded", function() {
    // Wait for widget to be initialized
    const waitForWidget = setInterval(() => {
        if (window.chatWidget) {
            clearInterval(waitForWidget);

            let lastThemeChange = Date.now();

            // Set up theme change detection
            const observer = new MutationObserver((mutations) => {

                // Find the mutation where a label becomes visible (hidden attribute is removed)
                const themeChange = mutations.find(mutation =>
                    mutation.target.classList.contains("md-header__button") &&
                    mutation.attributeName === "hidden" &&
                    !mutation.target.hasAttribute("hidden")  // Only process the label becoming visible
                );

                if (themeChange && Date.now() - lastThemeChange > 100) {
                    lastThemeChange = Date.now();
                    window.chatWidget.switchTheme();
                }
            });

            // Find and observe both theme toggle labels
            const themeToggles = document.querySelectorAll("label.md-header__button.md-icon");

            if (themeToggles.length > 0) {
                themeToggles.forEach(toggle => {
                    observer.observe(toggle, {
                        attributes: true,
                        attributeOldValue: true,
                        attributeFilter: ["hidden", "title"]
                    });
                });

                // Set initial theme based on current MkDocs theme
                const htmlElement = document.querySelector("body");
                const currentTheme = htmlElement.getAttribute("data-md-color-scheme");
                const widgetShouldBeDark = currentTheme === "slate";
                const widgetIsDark = !window.chatWidget.lightMode;

                // Only switch if there"s a mismatch
                if (widgetShouldBeDark !== widgetIsDark) {
                    window.chatWidget.switchTheme();
                }
            } else {
                console.error("Theme toggle labels not found!");
            }
        }
    }, 1000);

    // Add a timeout to stop checking after 30 seconds
    setTimeout(() => {
        if (!window.chatWidget) {
            console.error("Widget not found after 30 seconds");
            clearInterval(waitForWidget);
        }
    }, 30000);
});
