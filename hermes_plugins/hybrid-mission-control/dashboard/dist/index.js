(function () {
  var SDK = window.__HERMES_PLUGIN_SDK__;
  var React = SDK.React;
  var h = React.createElement;
  var Card = SDK.components.Card;
  var CardHeader = SDK.components.CardHeader;
  var CardTitle = SDK.components.CardTitle;
  var CardContent = SDK.components.CardContent;
  var Badge = SDK.components.Badge;
  var Button = SDK.components.Button;
  var Input = SDK.components.Input;
  var Separator = SDK.components.Separator;
  var apiBase = "/api/plugins/hybrid-mission-control";
  var priorities = ["P0_IMMEDIATE", "P1_HIGH", "P2_NORMAL", "P3_BACKGROUND"];
  var manualStatuses = ["TODO", "IN_PROGRESS", "BLOCKED", "DONE"];

  function api(path, options) {
    return SDK.fetchJSON(apiBase + path, options || {});
  }

  function post(path, body) {
    return api(path, {
      method: "POST",
      headers: {"Content-Type": "application/json"},
      body: JSON.stringify(body || {})
    });
  }

  function fmt(value) {
    if (value === null || value === undefined || value === "") return "None";
    if (typeof value === "number") return Number.isInteger(value) ? String(value) : value.toFixed(2);
    return String(value);
  }

  function priorityTone(priority) {
    if (priority === "P0_IMMEDIATE") return "mc-priority mc-p0";
    if (priority === "P1_HIGH") return "mc-priority mc-p1";
    if (priority === "P2_NORMAL") return "mc-priority mc-p2";
    return "mc-priority";
  }

  function ShellCard(props) {
    return h(Card, {className: "mc-card " + (props.className || "")},
      props.title ? h(CardHeader, {className: "mc-card-head"},
        h(CardTitle, {className: "mc-card-title"}, props.title),
        props.aside ? h("span", {className: "mc-card-aside"}, props.aside) : null
      ) : null,
      h(CardContent, {className: "mc-card-content"}, props.children)
    );
  }

  function Kpis(props) {
    var overview = props.overview || {};
    var items = [
      ["Runtime", overview.runtime_status || "UNKNOWN"],
      ["Gates", overview.pending_gates || 0],
      ["Harvests", overview.pending_harvests || 0],
      ["Replay", overview.replay_readiness || "UNKNOWN"],
      ["Milestones", overview.milestone_health || "UNKNOWN"],
      ["Load", fmt(overview.operator_load_hours) + "h"]
    ];
    return h("div", {className: "mc-kpis"}, items.map(function (item) {
      return h("div", {className: "mc-kpi", key: item[0]},
        h("span", null, item[0]),
        h("strong", null, item[1])
      );
    }));
  }

  function SectionTabs(props) {
    var tabs = [
      ["overview", "Overview"],
      ["workflow", "Workflow"],
      ["projects", "Projects"],
      ["tasks", "Tasks"],
      ["decisions", "Decisions"]
    ];
    return h("div", {className: "mc-tabs"}, tabs.map(function (tab) {
      return h("button", {
        key: tab[0],
        className: props.active === tab[0] ? "mc-tab active" : "mc-tab",
        onClick: function () { props.onChange(tab[0]); }
      }, tab[1]);
    }));
  }

  function Workflow(props) {
    var steps = (((props.snapshot || {}).workflow || {}).steps || []);
    return h("div", {className: "mc-workflow"}, steps.map(function (step) {
      var details = Object.keys(step.detail || {}).slice(0, 5);
      return h("div", {className: "mc-flow-step", key: step.id},
        h("div", {className: "mc-flow-label"}, step.label),
        h("div", {className: "mc-flow-count"}, step.count || 0),
        h("div", {className: "mc-mini-list"}, details.length ? details.map(function (key) {
          return h("div", {className: "mc-mini-row", key: key},
            h("span", null, key),
            h("strong", null, step.detail[key])
          );
        }) : h("span", {className: "mc-muted"}, "No active items"))
      );
    }));
  }

  function PrioritySelect(props) {
    return h("select", {
      className: "mc-select",
      value: props.value || "P3_BACKGROUND",
      onChange: function (event) { props.onChange(event.target.value); }
    }, priorities.map(function (priority) {
      return h("option", {key: priority, value: priority}, priority.replace("P", "P").replace("_", " "));
    }));
  }

  function ProjectCard(props) {
    var card = props.card;
    return h("div", {className: "mc-board-card"},
      h("div", {className: "mc-card-top"},
        h("span", {className: priorityTone(card.priority)}, card.priority),
        card.pending_gate_count ? h("span", {className: "mc-pill danger"}, card.pending_gate_count + " gate") : null
      ),
      h("h4", null, card.name),
      h("p", null, card.thesis || "No thesis recorded."),
      h("div", {className: "mc-meta-grid"},
        h("span", null, "Phase"), h("strong", null, card.phase_name || card.status),
        h("span", null, "Cashflow"), h("strong", null, "$" + fmt(card.cashflow_actual_usd || 0)),
        h("span", null, "Burn"), h("strong", null, card.executor_burn_ratio === null ? "n/a" : Math.round(card.executor_burn_ratio * 100) + "%")
      ),
      h("div", {className: "mc-control-row"},
        h(PrioritySelect, {value: card.priority, onChange: function (priority) {
          props.onPriority(card.project_id, priority);
        }})
      )
    );
  }

  function Board(props) {
    var lanes = (((props.snapshot || {}).project_board || {}).lanes || []);
    return h("div", {className: "mc-board"}, lanes.filter(function (lane) {
      return lane.count > 0 || ["PIPELINE", "BUILD", "OPERATE", "KILL_REVIEW"].indexOf(lane.id) !== -1;
    }).map(function (lane) {
      return h("section", {className: "mc-lane", key: lane.id},
        h("h3", null, h("span", null, lane.label), h("small", null, lane.count)),
        h("div", {className: "mc-card-stack"}, (lane.cards || []).length ? lane.cards.map(function (card) {
          return h(ProjectCard, {key: card.project_id, card: card, onPriority: props.onProjectPriority});
        }) : h("div", {className: "mc-empty"}, "Nothing here yet"))
      );
    }));
  }

  function TaskCard(props) {
    var task = props.task;
    return h("div", {className: "mc-task"},
      h("div", {className: "mc-card-top"},
        h(Badge, null, task.source),
        h("span", {className: priorityTone(task.priority)}, task.priority)
      ),
      h("h4", null, task.title),
      h("p", null, task.details || "No details."),
      h("div", {className: "mc-control-row"},
        h(PrioritySelect, {value: task.priority, onChange: function (priority) {
          props.onPriority(task, priority);
        }}),
        task.kind === "manual" ? h("select", {
          className: "mc-select",
          value: task.status,
          onChange: function (event) { props.onManualStatus(task.id, event.target.value); }
        }, manualStatuses.map(function (status) {
          return h("option", {key: status, value: status}, status.replace("_", " "));
        })) : null
      )
    );
  }

  function Tasks(props) {
    var lanes = (((props.snapshot || {}).tasks || {}).lanes || []);
    return h("div", {className: "mc-task-layout"},
      h("form", {className: "mc-task-form", onSubmit: props.onCreateManualTask},
        h("h3", null, "Add Operator Task"),
        h(Input, {name: "title", placeholder: "What needs operator attention?", required: true}),
        h("textarea", {name: "details", placeholder: "Optional detail"}),
        h("div", {className: "mc-control-row"},
          h("select", {name: "priority", className: "mc-select", defaultValue: "P2_NORMAL"},
            priorities.map(function (priority) { return h("option", {key: priority, value: priority}, priority); })
          ),
          h(Button, {type: "submit"}, "Add task")
        )
      ),
      h("div", {className: "mc-task-lanes"}, lanes.map(function (lane) {
        return h("section", {className: "mc-lane", key: lane.id},
          h("h3", null, h("span", null, lane.label), h("small", null, lane.count)),
          h("div", {className: "mc-card-stack"}, (lane.cards || []).length ? lane.cards.map(function (task) {
            return h(TaskCard, {
              key: task.kind + ":" + task.id,
              task: task,
              onPriority: props.onTaskPriority,
              onManualStatus: props.onManualStatus
            });
          }) : h("div", {className: "mc-empty"}, "No tasks"))
        );
      }))
    );
  }

  function Decisions(props) {
    var decisions = ((props.snapshot || {}).decisions || {});
    var gates = decisions.pending_gates || [];
    var g3 = decisions.pending_g3_requests || [];
    var quarantines = decisions.pending_quarantines || [];
    var halts = decisions.runtime_halts || [];
    return h("div", {className: "mc-decision-grid"},
      decisionList("Pending Gates", gates, function (item) {
        return [item.gate_type, item.trigger_description, item.project_name || item.project_id || "No project"];
      }),
      decisionList("G3 Spend Requests", g3, function (item) {
        return [item.request_id || item.approval_id || "G3", item.reason || item.task_summary || "Approval required", "Read-only until dashboard gate validation"];
      }),
      decisionList("Quarantines", quarantines, function (item) {
        return [item.quarantine_id || "Quarantine", item.reason || item.block_reason || "Pending review", "Read-only until dashboard gate validation"];
      }),
      decisionList("Runtime Halts", halts, function (item) {
        return [item.event_id || "Runtime halt", item.reason || item.trigger || "Active halt", item.status || "ACTIVE"];
      })
    );
  }

  function decisionList(title, items, mapper) {
    return h(ShellCard, {title: title, aside: String(items.length)},
      h("div", {className: "mc-card-stack"}, items.length ? items.map(function (item, index) {
        var parts = mapper(item);
        return h("div", {className: "mc-decision", key: parts[0] + index},
          h("strong", null, parts[0]),
          h("p", null, parts[1]),
          h("small", null, parts[2])
        );
      }) : h("div", {className: "mc-empty"}, "Clear"))
    );
  }

  function Overview(props) {
    var snapshot = props.snapshot || {};
    var alerts = snapshot.alerts || [];
    var digest = snapshot.latest_digest;
    return h("div", {className: "mc-overview-grid"},
      h(ShellCard, {title: "System Pulse", aside: snapshot.generated_at ? SDK.utils.isoTimeAgo(snapshot.generated_at) : "Live"},
        h(Kpis, {overview: snapshot.overview})
      ),
      h(ShellCard, {title: "Latest Digest"},
        digest ? h("pre", {className: "mc-digest"}, JSON.stringify(digest, null, 2)) : h("div", {className: "mc-empty"}, "No digest yet")
      ),
      h(ShellCard, {title: "Alerts", aside: String(alerts.length)},
        h("div", {className: "mc-card-stack"}, alerts.length ? alerts.map(function (alert) {
          return h("div", {className: "mc-alert", key: alert.alert_id || alert.created_at},
            h("strong", null, alert.alert_type || alert.type || "Alert"),
            h("p", null, alert.message || alert.trigger_description || "No message"),
            alert.acknowledged_at ? null : h(Button, {
              variant: "secondary",
              onClick: function () { props.onAckAlert(alert.alert_id); }
            }, "Acknowledge")
          );
        }) : h("div", {className: "mc-empty"}, "No active alerts"))
      )
    );
  }

  function MissionControl() {
    var useState = SDK.hooks.useState;
    var useEffect = SDK.hooks.useEffect;
    var state = useState(null);
    var snapshot = state[0];
    var setSnapshot = state[1];
    var tabState = useState("overview");
    var activeTab = tabState[0];
    var setActiveTab = tabState[1];
    var errorState = useState(null);
    var error = errorState[0];
    var setError = errorState[1];

    function refresh() {
      return api("/snapshot").then(function (payload) {
        setSnapshot(payload);
        setError(null);
      }).catch(function (err) {
        setError(String(err.message || err));
      });
    }

    useEffect(function () {
      refresh();
      var timer = setInterval(refresh, 5000);
      return function () { clearInterval(timer); };
    }, []);

    function run(action) {
      return action().then(refresh).catch(function (err) {
        setError(String(err.message || err));
      });
    }

    function body() {
      if (!snapshot) return h("div", {className: "mc-loading"}, "Loading Mission Control...");
      if (activeTab === "workflow") return h(Workflow, {snapshot: snapshot});
      if (activeTab === "projects") return h(Board, {
        snapshot: snapshot,
        onProjectPriority: function (projectId, priority) {
          return run(function () { return post("/projects/" + encodeURIComponent(projectId) + "/priority", {priority: priority}); });
        }
      });
      if (activeTab === "tasks") return h(Tasks, {
        snapshot: snapshot,
        onCreateManualTask: function (event) {
          event.preventDefault();
          var form = event.currentTarget;
          var data = new FormData(form);
          return run(function () {
            return post("/manual-tasks", {
              title: data.get("title"),
              details: data.get("details"),
              priority: data.get("priority")
            });
          }).then(function () { form.reset(); });
        },
        onTaskPriority: function (task, priority) {
          if (task.kind === "manual") {
            return run(function () { return post("/manual-tasks/" + encodeURIComponent(task.id), {priority: priority}); });
          }
          return run(function () { return post("/tasks/priority", {kind: task.kind, id: task.id, priority: priority}); });
        },
        onManualStatus: function (taskId, status) {
          return run(function () { return post("/manual-tasks/" + encodeURIComponent(taskId), {status: status}); });
        }
      });
      if (activeTab === "decisions") return h(Decisions, {snapshot: snapshot});
      return h(Overview, {
        snapshot: snapshot,
        onAckAlert: function (alertId) {
          if (!alertId) return Promise.resolve();
          return run(function () { return post("/alerts/" + encodeURIComponent(alertId) + "/ack", {}); });
        }
      });
    }

    return h("div", {className: "mc-root"},
      h("header", {className: "mc-hero"},
        h("div", null,
          h("p", {className: "mc-eyebrow"}, "Hybrid Autonomous AI"),
          h("h1", null, "Mission Control"),
          h("p", {className: "mc-subtitle"}, "A Hermes-native operator tab for workflow state, boards, priorities, and decision pressure.")
        ),
        h("div", {className: "mc-hero-note"},
          h("strong", null, "Gate-safe v1"),
          h("span", null, "Gates and quarantines are visible but read-only until dashboard auth and audit validation pass.")
        )
      ),
      error ? h("div", {className: "mc-error"}, error) : null,
      h(SectionTabs, {active: activeTab, onChange: setActiveTab}),
      h(Separator, {className: "mc-separator"}),
      body()
    );
  }

  window.__HERMES_PLUGINS__.register("hybrid-mission-control", MissionControl);
})();
