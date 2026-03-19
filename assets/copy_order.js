// Copy order number to clipboard when clicking the clipboard icon
document.addEventListener('click', function(e) {
    var btn = e.target.closest('.copy-btn');
    if (btn) {
        var text = btn.getAttribute('data-clipboard-text');
        if (text) {
            navigator.clipboard.writeText(text).then(function() {
                // Brief visual feedback
                var orig = btn.textContent;
                btn.textContent = '\u2713';
                btn.style.opacity = '1';
                setTimeout(function() {
                    btn.textContent = orig;
                    btn.style.opacity = '0.5';
                }, 1000);
            });
        }
    }
});
