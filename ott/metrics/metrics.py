import subprocess

def get_latency(interface="eth0"):
    try:
        result = subprocess.run(
            ["tc", "qdisc", "show", "dev", interface],
            capture_output=True, text=True
        )
        output = result.stdout
        # parse do delay (ex: delay 10ms)
        import re
        m = re.search(r"delay (\d+)ms", output)
        if m:
            return int(m.group(1))
    except Exception:
        passfa
    return None

def get_packet_loss(target_ip):
    try:
        result = subprocess.run(
            ["ping", "-c", "5", target_ip],
            capture_output=True, text=True
        )
        output = result.stdout
        # extrai percentual de perda
        import re
        m = re.search(r"(\d+)% packet loss", output)
        if m:
            return float(m.group(1))
    except Exception:
        pass
    return None

def get_bandwidth(interface="eth0"):
    try:
        result = subprocess.run(
            ["tc", "class", "show", "dev", interface],
            capture_output=True, text=True
        )
        # parse da saída para descobrir bandwidth
        return result.stdout  # ou extrair o valor específico
    except Exception:
        return None
