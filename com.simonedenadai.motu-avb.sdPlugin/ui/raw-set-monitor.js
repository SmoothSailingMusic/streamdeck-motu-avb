const { streamDeckClient } = SDPIComponents;

streamDeckClient.getGlobalSettings().then((globalSettings) => {
    const motuUrlInput = document.getElementById('motuUrl');
    if (globalSettings.motuUrl) {
        motuUrlInput.value = globalSettings.motuUrl;
    }

    document.getElementById('connection-button')?.addEventListener('click', (_event) => {
        const motuUrl = motuUrlInput.value.replace(/\/+$/, "");
        if (!motuUrl) return;
        fetch(motuUrl, { method: 'GET' })
            .then((response) => response.json())
            .then((json) => {
                if (json.uid) {
                    streamDeckClient.setGlobalSettings({ ...globalSettings, motuUrl: motuUrl });
                    motuUrlInput.classList.add('valid');
                } else {
                    motuUrlInput.classList.add('invalid');
                }
            })
            .catch(() => motuUrlInput.classList.add('invalid'));
    });
});
