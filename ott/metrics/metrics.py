import subprocess
import re
from typing import Optional

def _run_cmd(cmd, timeout=3):
    try:
        res = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        return res.stdout
    except Exception:
        return ""

def get_latency(target_ip: str) -> Optional[float]:
    """
    Retorna latência média em ms para target_ip usando ping (-c 3).
    Se falhar, devolve None.
    """
    if not target_ip:
        return None
    out = _run_cmd(["ping", "-c", "3", "-W", "1", target_ip], timeout=5)
    # procura linha: rtt min/avg/max/mdev = 0.041/0.041/0.041/0.000 ms
    m = re.search(r"rtt [\w/]+ = [\d\.]+/([\d\.]+)/", out)
    if m:
        try:
            return float(m.group(1))
        except:
            return None
    # em alguns sistemas a saída pode ser "round-trip min/avg/max/stddev"
    m2 = re.search(r"round-trip [\w/]+ = [\d\.]+/([\d\.]+)/", out)
    if m2:
        try:
            return float(m2.group(1))
        except:
            return None
    return None

def get_packet_loss(target_ip: str) -> Optional[float]:
    """
    Retorna percentagem de packet loss (0..100) para target_ip usando ping.
    Se falhar, devolve None.
    """
    if not target_ip:
        return None
    out = _run_cmd(["ping", "-c", "3", "-W", "1", target_ip], timeout=5)
    # procura "X% packet loss"
    m = re.search(r"(\d+(?:\.\d+)?)% packet loss", out)
    if m:
        try:
            return float(m.group(1))
        except:
            return None
    return None

def get_bandwidth(target_ip: str) -> Optional[float]:
    """
    Tenta inferir banda em Mbps para a interface local ligada ao target_ip.
    Isto é frágil — tentamos:
     - resolver a interface pelo IP de rota (ip route get)
     - ler /sys/class/net/<iface>/speed (valor em Mbps) se existir
     - fallback: tentar 'ethtool <iface>' e parsear 'Speed: 1000Mb/s'
    Devolve um float (Mbps) ou None se não conseguirmos.
    """
    if not target_ip:
        return None
    # descobrir interface usada para chegar ao target
    try:
        out = _run_cmd(["ip", "route", "get", target_ip], timeout=2)
        # saída ex: "target_ip via ... dev eth0 src x.x.x.x ..."
        m = re.search(r"dev (\S+)", out)
        if not m:
            return None
        iface = m.group(1)
    except Exception:
        return None

    # tenta /sys/class/net/<iface>/speed
    try:
        path = f"/sys/class/net/{iface}/speed"
        with open(path, "r") as f:
            val = f.read().strip()
            if val.isdigit():
                return float(val)  # Mbps
    except Exception:
        pass

    # tenta ethtool
    try:
        out2 = _run_cmd(["ethtool", iface], timeout=2)
        m2 = re.search(r"Speed:\s*([0-9]+)Mb/s", out2)
        if m2:
            return float(m2.group(1))
    except Exception:
        pass

    return None
