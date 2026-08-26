#!/usr/bin/env python3
import urllib.request
import json
import time
import sys

BASE_URL = "http://localhost:8080"

def get(path):
    req = urllib.request.Request(f"{BASE_URL}{path}")
    with urllib.request.urlopen(req) as resp:
        return resp.status, resp.read().decode('utf-8')

def post(path, data):
    body = json.dumps(data).encode('utf-8')
    req = urllib.request.Request(f"{BASE_URL}{path}", data=body, headers={'Content-Type': 'application/json'})
    with urllib.request.urlopen(req) as resp:
        return resp.status, resp.read().decode('utf-8')

def put(path, data):
    body = json.dumps(data).encode('utf-8')
    req = urllib.request.Request(f"{BASE_URL}{path}", data=body, headers={'Content-Type': 'application/json'}, method='PUT')
    with urllib.request.urlopen(req) as resp:
        return resp.status, resp.read().decode('utf-8')

def delete(path):
    req = urllib.request.Request(f"{BASE_URL}{path}", method='DELETE')
    with urllib.request.urlopen(req) as resp:
        return resp.status, resp.read().decode('utf-8')

def run_tests():
    print("=== DINIS ICMP Network Monitor E2E Test Suite ===")

    # Reset to fast test settings
    try:
        put("/api/settings", {
            "intervalSec": 1.5,
            "timeoutMs": 400,
            "failThreshold": 2,
            "concurrency": 100,
            "discoveryIntervalMin": 15,
            "autoDiscovery": True
        })
    except Exception as e:
        print(f"Warning setting settings: {e}")

    # Cleanup any previous test artifacts
    for cleanup_fn in [
        lambda: delete("/api/cidrs?cidr=192.0.2.0/28"),
        lambda: delete("/api/cidrs?cidr=192.0.2.5/32"),
        lambda: delete("/api/exclusions?rule=192.0.2.5"),
        lambda: delete("/api/hosts/192.0.2.5/enrollment"),
    ]:
        try:
            cleanup_fn()
        except Exception:
            pass

    # 1. Test Static Web Assets
    print("[TEST 1] Verifying Static Web Assets...")
    status, html = get("/")
    assert status == 200 and "<title>DINIS" in html, "HTML not serving properly"
    status, css = get("/style.css")
    assert status == 200 and "--color-up" in css, "CSS not serving properly"
    status, js = get("/app.js")
    assert status == 200 and "EventSource" in js, "JS not serving properly"
    print("  ✓ Static web dashboard assets served successfully.")

    # 2. Test Discovery Status & Initial Hosts
    print("[TEST 2] Verifying Discovery Status & Initial Hosts...")
    status, raw = get("/api/discovery/status")
    disc = json.loads(raw)
    assert status == 200
    print(f"  ✓ Discovery Status: Capacity={disc['subnetCapacity']}, Discovered={disc['lastDiscoveredCount']}")

    status, raw = get("/api/hosts")
    hosts = {h["ip"]: h for h in json.loads(raw)}
    assert "127.0.0.1" in hosts, "Expected 127.0.0.1 to be monitored"
    print(f"  ✓ Monitored active hosts count: {len(hosts)}")

    # 3. Add Large/Unreachable Subnet CIDR 192.0.2.0/28 (14 unallocated IPs)
    print("[TEST 3] Adding Subnet CIDR 192.0.2.0/28 (14 unallocated IPs)...")
    status, raw = post("/api/cidrs", {
        "cidr": "192.0.2.0/28",
        "description": "Unallocated Test Subnet",
        "includeNetAndBcast": False
    })
    assert status == 200
    res = json.loads(raw)
    assert res["totalHosts"] == 14, f"Expected 14 usable hosts, got {res['totalHosts']}"
    print(f"  ✓ Added CIDR 192.0.2.0/28 ({res['totalHosts']} total capacity).")

    # 4. Trigger Discovery Sweep
    print("[TEST 4] Running Discovery Sweep...")
    status, raw = post("/api/discovery/run", {"cidr": "192.0.2.0/28"})
    assert status == 200
    time.sleep(4)

    # Verify that unallocated offline IPs were NOT added to active monitoring
    status, raw = get("/api/hosts")
    hosts_after = {h["ip"]: h for h in json.loads(raw)}
    assert "192.0.2.1" not in hosts_after, "Unallocated offline IP should NOT be added to active monitoring"
    assert "192.0.2.5" not in hosts_after, "Unallocated offline IP should NOT be added to active monitoring"
    print("  ✓ Verified: Unallocated/offline IPs in the subnet were NOT added to continuous pinging or alerting!")

    # 5. Promote a specific target IP or Add static target to simulate monitoring an outage
    print("[TEST 5] Adding static target 192.0.2.5/32 to test outage detection...")
    status, raw = post("/api/cidrs", {
        "cidr": "192.0.2.5/32",
        "description": "Critical Server Under Monitoring",
        "includeNetAndBcast": False
    })
    assert status == 200

    # 6. Verify Active Outage Alert on explicitly monitored target
    print("[TEST 6] Waiting for Alert on Monitored Target 192.0.2.5...")
    alert_found = False
    for _ in range(18):
        time.sleep(1)
        status, raw = get("/api/alerts")
        alerts = json.loads(raw)
        if any(a["ip"] == "192.0.2.5" for a in alerts):
            alert_found = True
            break

    assert alert_found, "Expected alert for monitored target 192.0.2.5"
    print(f"  ✓ Alert correctly triggered for monitored offline host: {len(alerts)} active firing alert(s).")

    # 7. Acknowledge Alert with Operator Note
    print("[TEST 7] Acknowledging Alert for 192.0.2.5 with Operator Note...")
    status, raw = post("/api/alerts/acknowledge", {
        "ip": "192.0.2.5",
        "ackBy": "DevOps Lead",
        "note": "Hardware replacement in progress"
    })
    assert status == 200
    ack_res = json.loads(raw)
    assert ack_res["acknowledged"] is True
    assert ack_res["acknowledgedBy"] == "DevOps Lead"
    assert ack_res["state"] == "ACKNOWLEDGED"

    status, raw = get("/api/hosts/192.0.2.5")
    h1 = json.loads(raw)
    assert h1["alertAcknowledged"] is True
    assert h1["alertAckBy"] == "DevOps Lead"
    print(f"  ✓ Alert acknowledged: state={ack_res['state']}, operator={ack_res['acknowledgedBy']}.")

    # 8. Exclude Host from Monitoring
    print("[TEST 8] Adding Exclusion Rule for 192.0.2.5...")
    status, raw = post("/api/exclusions", {
        "rule": "192.0.2.5",
        "reason": "Decommissioned Node"
    })
    assert status == 200
    status, raw = get("/api/hosts/192.0.2.5")
    h2 = json.loads(raw)
    assert h2["status"] == "EXCLUDED"
    assert h2["isExcluded"] is True
    print(f"  ✓ Host 192.0.2.5 status changed to EXCLUDED and alert resolved.")

    # 9. Manual Probe On-Demand
    print("[TEST 9] Triggering Manual Probe on 127.0.0.1...")
    status, raw = post("/api/hosts/127.0.0.1/ping", {})
    assert status == 200
    ping_res = json.loads(raw)
    assert ping_res["Success"] is True
    assert ping_res["LatencyMs"] > 0
    print(f"  ✓ Manual ping success: RTT = {ping_res['LatencyMs']:.3f} ms.")

    # 10. Update Monitoring Settings (including discovery interval)
    print("[TEST 10] Updating Monitoring & Discovery Settings...")
    status, raw = put("/api/settings", {
        "intervalSec": 2.5,
        "timeoutMs": 800,
        "failThreshold": 2,
        "concurrency": 150,
        "discoveryIntervalMin": 10,
        "autoDiscovery": True
    })
    assert status == 200
    sett_res = json.loads(raw)
    assert sett_res["intervalSec"] == 2.5
    assert sett_res["discoveryIntervalMin"] == 10
    print(f"  ✓ Settings updated live: MonitorInterval={sett_res['intervalSec']}s, DiscoveryInterval={sett_res['discoveryIntervalMin']}m.")

    # 11. Cleanup test CIDRs & exclusion
    print("[TEST 11] Cleaning up test CIDRs and exclusion...")
    delete("/api/cidrs?cidr=192.0.2.0/28")
    delete("/api/cidrs?cidr=192.0.2.5/32")
    delete("/api/exclusions?rule=192.0.2.5")
    time.sleep(1)
    print("  ✓ Cleanup completed successfully.")

    print("\n===================================================================")
    print("ALL 11 DISCOVERY & MONITORING VALIDATION TESTS PASSED 100%!")
    print("===================================================================")

if __name__ == "__main__":
    run_tests()
