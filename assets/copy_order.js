// Copy order number when clicking on Order # column in the DataTable
document.addEventListener('click', function(e) {
    // Handle DataTable cell clicks — check if it's in the Order # column
    var cell = e.target.closest('td');
    if (cell) {
        var row = cell.closest('tr');
        var table = cell.closest('#per-order-profit-table');
        if (table && row) {
            // Check if this is the first column (Order #)
            var cells = row.querySelectorAll('td');
            if (cells.length > 0 && cells[0] === cell) {
                var text = cell.textContent.trim();
                if (text && /^\d+$/.test(text)) {
                    navigator.clipboard.writeText(text).then(function() {
                        var orig = cell.style.color;
                        cell.textContent = '\u2713 Copied!';
                        cell.style.color = '#2ecc71';
                        setTimeout(function() {
                            cell.textContent = text;
                            cell.style.color = orig;
                        }, 1000);
                    });
                }
            }
        }
    }

    // Also handle old-style copy buttons
    var btn = e.target.closest('.copy-btn');
    if (btn) {
        var clipText = btn.getAttribute('data-clipboard-text');
        if (clipText) {
            navigator.clipboard.writeText(clipText).then(function() {
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
