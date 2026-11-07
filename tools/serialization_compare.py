"""Bandingkan ukuran & waktu serialisasi event yang sama: JSON vs XML (stdlib saja)."""
import json
import timeit
import xml.etree.ElementTree as ET

event = {
    "event_id": "1:order.created", "type": "order.created", "order_id": 1,
    "customer": "Sari", "email": "sari@mail.com",
    "items": [{"sku": "A1", "qty": 2}, {"sku": "B2", "qty": 5}], "total": 125000, "ts": 1760000000.0,
}


def to_xml(d):
    root = ET.Element("event")
    for k, v in d.items():
        if k == "items":
            items = ET.SubElement(root, "items")
            for it in v:
                e = ET.SubElement(items, "item")
                for ik, iv in it.items():
                    ET.SubElement(e, ik).text = str(iv)
        else:
            ET.SubElement(root, k).text = str(v)
    return ET.tostring(root)


rows = [
    ("JSON (compact)", lambda: json.dumps(event, separators=(",", ":")).encode()),
    ("JSON (default)", lambda: json.dumps(event).encode()),
    ("XML", lambda: to_xml(event)),
]
print(f"{'Format':<16}{'Ukuran (byte)':>15}{'Waktu 10rb x (dtk)':>22}")
for name, fn in rows:
    size = len(fn())
    t = timeit.timeit(fn, number=10_000)
    print(f"{name:<16}{size:>15}{t:>22.3f}")
