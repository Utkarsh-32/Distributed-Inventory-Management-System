const clusterSummary = document.getElementById("cluster-summary");
const clusterRows = document.getElementById("cluster-nodes");

async function refreshCluster() {
    try {
        const response = await fetch("/api/cluster/status", {cache: "no-store"});
        if (!response.ok) throw new Error("Status unavailable");
        const {nodes} = await response.json();
        const rows = nodes.map(node => {
            const row = document.createElement("tr");
            const values = node.reachable
                ? [`Node ${node.node_id} (${node.address})`, node.role, node.term,
                   node.leader_id, node.commit_index, node.last_applied]
                : [node.address, "Unreachable", null, null, null, null];
            for (const value of values) {
                const cell = row.insertCell();
                cell.textContent = value ?? "—";
            }
            return row;
        });
        clusterRows.replaceChildren(...rows);
        clusterSummary.textContent = `${nodes.filter(node => node.reachable).length} of ${nodes.length} nodes reachable`;
    } catch {
        clusterRows.replaceChildren();
        clusterSummary.textContent = "Cluster status unavailable. Retrying…";
    } finally {
        window.setTimeout(refreshCluster, 2000);
    }
}

refreshCluster();
