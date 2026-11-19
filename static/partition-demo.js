(() => {
    const panel = document.querySelector("#partition-demo");
    const status = panel.querySelector("#partition-status");
    const start = panel.querySelector("[data-action=isolate]");
    const reconnect = panel.querySelector("[data-action=reconnect]");
    const phases = {
        idle: "Ready to start.", busy: "Another session is running the demo.",
        starting: "Starting three nodes and waiting for their first election…",
        electing: "Leader isolated. Waiting for the other two nodes to elect a leader and process their purchase.",
        partitioned: "The majority sold the laptop. Reconnect to bring the isolated node up to date. Links restore automatically after two minutes.",
        recovering: "Links restored. Checking each node’s order, stock, and Raft log…",
        complete: "Verified on all three nodes: one successful purchase, zero laptops left, and identical logs."
    };
    let timer;
    let generation = 0;
    function render(data) {
        status.textContent = phases[data.phase] ?? data.message ?? "Demo unavailable.";
        start.disabled = !["idle", "complete", "error"].includes(data.phase);
        reconnect.disabled = data.phase !== "partitioned";
        panel.querySelector(".partition-results").hidden = !data.nodes.length;
        for (const line of panel.querySelectorAll("[data-link]")) {
            line.classList.toggle("cut", !!data.isolated && line.dataset.link.includes(String(data.isolated)));
        }
        const body = panel.querySelector("#partition-nodes");
        body.replaceChildren();
        for (const node of data.nodes) {
            const isolated = node.id === data.isolated;
            panel.querySelector('[data-id="' + node.id + '"]').classList.toggle("cut", isolated);
            const row = body.insertRow();
            for (const value of [node.id, node.role + (isolated ? " · no majority" : ""), node.term,
                node.committed + " / " + node.applied, node.stock, node.sales.length]) {
                row.insertCell().textContent = value;
            }
        }
        const list = panel.querySelector("#partition-orders");
        list.replaceChildren();
        for (const [side, order] of Object.entries(data.orders)) {
            const li = document.createElement("li");
            const node = data.nodes.find(n => n.id === order.node);
            const applied = node?.sales.includes("order:1:" + side);
            const result = order.status === "NO_QUORUM" ? "No majority · purchase uncommitted"
                : applied && order.committed ? "Committed and applied" : order.status;
            li.textContent = (side === "minority" ? "Isolated side" : "Majority side")
                + " · node " + order.node + " — " + result
                + (order.index ? " (log entry " + order.index + ", term " + order.term + ")" : "");
            list.append(li);
        }
    }
    async function refresh(action) {
        const current = ++generation;
        clearTimeout(timer);
        if (action) { start.disabled = true; reconnect.disabled = true; }
        try {
            const response = await fetch("/api/demo/partition" + (action ? "/" + action : ""), {
                method: action ? "POST" : "GET", cache: "no-store", headers: {"X-Inventory-Demo": "1"}
            });
            const data = await response.json();
            if (current !== generation) return;
            if (!response.ok) throw Error(data.detail || "Unable to load demo.");
            render(data);
            if (!["idle", "complete", "error"].includes(data.phase)) timer = setTimeout(refresh, 650);
        } catch (error) {
            if (current !== generation) return;
            status.textContent = error.message;
            if (action) timer = setTimeout(refresh, 1800);
            else start.disabled = false;
        }
    }
    start.addEventListener("click", () => refresh("isolate"));
    reconnect.addEventListener("click", () => refresh("reconnect"));
    refresh();
})();
