(() => {
    const tools = document.querySelector(".inventory-tools");
    if (!tools) return;
    const search = tools.querySelector("#product-search");
    const view = tools.querySelector("#stock-view");
    const threshold = Number(tools.dataset.threshold);
    const rows = Array.from(document.querySelectorAll("#inventory [data-product]"), row => ({
        row, name: row.dataset.product.toLowerCase(), stock: JSON.parse(row.dataset.stock)
    }));
    function filter() {
        const term = search.value.trim().toLowerCase();
        let visible = 0;
        for (const {row, name, stock} of rows) {
            row.hidden = !name.includes(term) || (view.value !== "" && (stock[view.value] ?? 0) > threshold);
            if (!row.hidden) visible++;
        }
        tools.querySelector("output").textContent = visible + " of " + rows.length + " products";
        document.querySelector("#inventory-empty").hidden = visible !== 0;
    }
    tools.hidden = false;
    search.addEventListener("input", filter);
    view.addEventListener("change", filter);
    filter();
})();
