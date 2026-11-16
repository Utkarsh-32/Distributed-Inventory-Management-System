(function () {
    const POLL_MS = 2000;
    const nodesBox = document.getElementById("cluster-nodes");
    const eventsBox = document.getElementById("cluster-events");
    const healthPill = document.getElementById("cluster-health");
    const meta = document.getElementById("cluster-meta");

    if (!nodesBox) {
        return;
    }

    function escapeHtml(text) {
        return String(text).replace(/[&<>"']/g, function (c) {
            return {"&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;"}[c];
        });
    }

    function nodeCard(node, leaderId) {
        const label = node.node_id ? "node" + node.node_id : node.address;

        if (!node.reachable) {
            return (
                '<div class="node-card node-down">' +
                '<div class="node-title">' + escapeHtml(label) +
                ' <span class="badge badge-cancelled">DOWN</span></div>' +
                '<div class="muted">' + escapeHtml(node.address) + '</div>' +
                '<div class="node-note">No response. Waiting for it to return…</div>' +
                '</div>'
            );
        }

        const isLeader = node.role === "leader" && node.node_id === leaderId;
        const roleClass = isLeader ? "badge-received"
            : node.role === "candidate" ? "badge-approved" : "badge-placed";
        const lag = Math.max(0, (node.last_log_index || 0) - (node.last_applied || 0));

        return (
            '<div class="node-card ' + (isLeader ? "node-leader" : "") + '">' +
            '<div class="node-title">' + escapeHtml(label) +
            ' <span class="badge ' + roleClass + '">' + escapeHtml(node.role.toUpperCase()) + '</span>' +
            (isLeader ? ' <span class="crown" title="Agreed leader">&#9733;</span>' : '') +
            '</div>' +
            '<div class="muted">' + escapeHtml(node.address) + '</div>' +
            '<dl class="node-stats">' +
            '<div><dt>Term</dt><dd>' + node.term + '</dd></div>' +
            '<div><dt>Leader</dt><dd>' + (node.leader_id ? "node" + node.leader_id : "—") + '</dd></div>' +
            '<div><dt>Log</dt><dd>' + (node.last_log_index || 0) + '</dd></div>' +
            '<div><dt>Committed</dt><dd>' + (node.commit_index || 0) + '</dd></div>' +
            '<div><dt>Applied</dt><dd>' + (node.last_applied || 0) +
            (lag ? ' <span class="status-low">(' + lag + ' behind)</span>' : '') + '</dd></div>' +
            '<div><dt>Voted for</dt><dd>' + (node.voted_for ? "node" + node.voted_for : "—") + '</dd></div>' +
            '</dl>' +
            '</div>'
        );
    }

    function render(data) {
        nodesBox.innerHTML = data.nodes.map(function (node) {
            return nodeCard(node, data.leader_id);
        }).join("");

        if (data.healthy) {
            healthPill.textContent = "healthy · leader node" + data.leader_id + " · term " + data.term;
            healthPill.className = "cluster-pill pill-ok";
        } else if (data.reachable_count >= data.quorum) {
            healthPill.textContent = "election in progress · term " + data.term;
            healthPill.className = "cluster-pill pill-warn";
        } else {
            healthPill.textContent = "no quorum · " + data.reachable_count + "/" + data.total_count + " nodes up";
            healthPill.className = "cluster-pill pill-bad";
        }

        meta.textContent = data.reachable_count + " of " + data.total_count +
            " nodes reachable · quorum " + data.quorum +
            " · updated " + new Date(data.timestamp * 1000).toLocaleTimeString();

        if (data.events.length) {
            eventsBox.innerHTML = data.events.map(function (event) {
                const when = new Date(event.time * 1000).toLocaleTimeString();
                return '<li class="event-' + escapeHtml(event.kind) + '">' +
                    '<span class="muted">' + when + '</span> ' + escapeHtml(event.message) + '</li>';
            }).join("");
        }
    }

    function poll() {
        fetch("/api/cluster", {cache: "no-store"})
            .then(function (response) {
                if (response.status === 401) {
                    window.location.href = "/login";
                    return null;
                }
                return response.json();
            })
            .then(function (data) {
                if (data) {
                    render(data);
                }
            })
            .catch(function () {
                healthPill.textContent = "web client unreachable";
                healthPill.className = "cluster-pill pill-bad";
            })
            .finally(function () {
                setTimeout(poll, POLL_MS);
            });
    }

    poll();
})();
