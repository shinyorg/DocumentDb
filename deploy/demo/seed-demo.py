#!/usr/bin/env python3
"""Emits seed-demo.sql - the documents the public playground opens on.

Covers the envelope table plus four sidecars, so the workspace tabs that only appear when their
backing data exists (History, Geometry, Blobs, Full text, Vectors) have something behind them:

  documents               the envelope - Order, Product, Customer
  documents_history       temporal versions, one open version per live document
  documents_blobs         product images / datasheets and order invoices
  documents_spatial_map   \\ R*Tree bounding boxes for the geometry in the bodies
  documents_spatial       /
  documents_fts           FTS5 over Product name/category/tags, with the per-type triggers that
                          maintain it - created before the inserts, so seeding populates it exactly
                          as a write from the library would

Products also carry an `embedding` array, which is what makes the Vectors tab appear. There is no
`documents_vec_Product` here: that is a vec0 virtual table and creating one needs the sqlite-vec
extension, which neither this script nor the container image loads. The tab is honest about that -
it reports the embeddings it finds in the bodies and says the sidecar is absent, which is the state
it exists to report.

Two things drive the shape of what is written here:

* The Generate tab learns from documents that already exist - DocumentGenerator draws numbers from
  the observed range, dates from the observed span, and categorical values from the set actually
  used. So the spread matters more than the volume. Counts stay under
  DocumentAdminService.SchemaSampleSize (200) so the shape is profiled from every document.

* Geometry is read from the document body, not from the spatial sidecar (see
  DocumentAdminService.Geometry: "The sidecar holds only bounding boxes for R*Tree / index pruning
  ... the GeoJSON in Data is the actual value"). So the geometry that matters is the GeoJSON in the
  bodies; the sidecar is written alongside it for realism, and because the explorer classifies
  _spatial* tables as sidecars rather than as documents to browse.

Seeded RNG: re-running produces a byte-identical .sql, so the diff is empty unless the shape
actually changed.

    python3 seed-demo.py > seed-demo.sql
"""

import hashlib
import json
import math
import struct
import zlib
from datetime import datetime, timedelta, timezone

import random

random.seed(20260731)

# Microsoft.Data.Sqlite's DateTimeOffset text format. The library binds DateTimeOffset parameters
# for CreatedAt/UpdatedAt and for the history sidecar's ValidFrom/ValidTo (SqliteDatabaseProvider's
# NormalizeParameterValue passes DateTimeOffset straight through), so rows written by hand have to
# match or Ado.Timestamp's DateTimeOffset.TryParse gets a different value back than it stored.
TS = "%Y-%m-%d %H:%M:%S.%f+00:00"
EPOCH = datetime(2026, 7, 31, 12, 0, 0, tzinfo=timezone.utc)

# Operation names from DocumentAdminService.Temporal - matched exactly so the tool and the library
# agree on what a version row means.
OP_INSERTED = "Inserted"
OP_UPDATED = "Updated"
OP_REMOVED = "Removed"

# The library takes the actor from the store's CaptureActor. These are the demo's stand-ins; the
# admin UI stamps its own writes with "shiny-docdb-myadmin", so leaving that name out of the seed
# keeps "what the tool did" distinguishable from "what the app did".
ACTORS = ["checkout-api", "fulfillment-worker", "catalog-sync", "support@example.com", "billing-job"]

documents = []   # (id, typeName, dataJson, createdAt, updatedAt)
history = []     # (id, typeName, version, validFrom, validTo, operation, actor, dataJson|None)
blobs = []       # (id, typeName, blobKey, payload, contentType, fileName, createdAt, updatedAt)
spatial = []     # (docId, typeName, minLat, maxLat, minLng, maxLng)


def fmt(dt):
    return dt.strftime(TS)


def dumps(body):
    return json.dumps(body, separators=(", ", ": "))


# ---------------------------------------------------------------- binary payloads

def png(width, height, pixel):
    """A real PNG - 8-bit RGB, no palette. Blob payloads in a demo should survive being downloaded."""
    raw = b"".join(
        b"\x00" + b"".join(bytes(pixel(x, y)) for x in range(width))
        for y in range(height)
    )

    def chunk(tag, data):
        body = tag + data
        return struct.pack(">I", len(data)) + body + struct.pack(">I", zlib.crc32(body) & 0xFFFFFFFF)

    return (
        b"\x89PNG\r\n\x1a\n"
        + chunk(b"IHDR", struct.pack(">IIBBBBB", width, height, 8, 2, 0, 0, 0))
        + chunk(b"IDAT", zlib.compress(raw, 9))
        + chunk(b"IEND", b"")
    )


def pdf(lines):
    """A one-page PDF with a correct xref table, so the download opens instead of erroring."""
    content = b"BT /F1 16 Tf 72 720 Td 18 TL\n"
    for line in lines:
        escaped = line.replace("\\", r"\\").replace("(", r"\(").replace(")", r"\)")
        content += f"({escaped}) Tj T*\n".encode("latin-1")
    content += b"ET\n"

    objects = [
        b"<</Type/Catalog/Pages 2 0 R>>",
        b"<</Type/Pages/Kids[3 0 R]/Count 1>>",
        b"<</Type/Page/Parent 2 0 R/MediaBox[0 0 612 792]"
        b"/Resources<</Font<</F1 4 0 R>>>>/Contents 5 0 R>>",
        b"<</Type/Font/Subtype/Type1/BaseFont/Helvetica>>",
        b"<</Length " + str(len(content)).encode() + b">>stream\n" + content + b"endstream",
    ]

    out = b"%PDF-1.4\n"
    offsets = []
    for i, obj in enumerate(objects, start=1):
        offsets.append(len(out))
        out += str(i).encode() + b" 0 obj" + obj + b"endobj\n"

    xref = len(out)
    out += b"xref\n0 " + str(len(objects) + 1).encode() + b"\n0000000000 65535 f \n"
    for offset in offsets:
        out += f"{offset:010d} 00000 n \n".encode()
    out += (
        b"trailer<</Size " + str(len(objects) + 1).encode() + b"/Root 1 0 R>>\n"
        b"startxref\n" + str(xref).encode() + b"\n%%EOF\n"
    )
    return out


# ---------------------------------------------------------------- geometry

CITIES = [
    ("Toronto", "ON", "CA", 43.6532, -79.3832),
    ("Vancouver", "BC", "CA", 49.2827, -123.1207),
    ("Montreal", "QC", "CA", 45.5019, -73.5674),
    ("Halifax", "NS", "CA", 44.6488, -63.5752),
    ("Seattle", "WA", "US", 47.6062, -122.3321),
    ("Austin", "TX", "US", 30.2672, -97.7431),
    ("Denver", "CO", "US", 39.7392, -104.9903),
    ("Chicago", "IL", "US", 41.8781, -87.6298),
]


def point(lat, lng):
    """GeoJSON is [longitude, latitude] - GeometryJsonConverter writes Longitude first."""
    return {"type": "Point", "coordinates": [round(lng, 5), round(lat, 5)]}


def polygon_around(lat, lng, radius):
    """A closed hexagonal ring. Rings must close (first == last) and hold at least 4 positions."""
    import math
    ring = []
    for i in range(6):
        angle = math.pi / 3 * i
        # Longitude degrees shrink with latitude; without the cos the "hexagon" is a squashed oval.
        ring.append([
            round(lng + radius * math.cos(angle) / math.cos(math.radians(lat)), 5),
            round(lat + radius * math.sin(angle), 5),
        ])
    ring.append(ring[0])
    return {"type": "Polygon", "coordinates": [ring]}


def route_between(lat, lng, stops):
    """A LineString needs at least two positions."""
    coords = []
    cur_lat, cur_lng = lat, lng
    for _ in range(stops):
        coords.append([round(cur_lng, 5), round(cur_lat, 5)])
        cur_lat += random.uniform(-0.09, 0.09)
        cur_lng += random.uniform(-0.12, 0.12)
    return {"type": "LineString", "coordinates": coords}


def envelope(geometry):
    """Flattens any of the shapes above to (minLat, maxLat, minLng, maxLng) for the R*Tree row."""
    def positions(node):
        if node and isinstance(node[0], (int, float)):
            yield node
            return
        for child in node:
            yield from positions(child)

    lats, lngs = [], []
    for lng, lat in positions(geometry["coordinates"]):
        lats.append(lat)
        lngs.append(lng)
    return min(lats), max(lats), min(lngs), max(lngs)


# ---------------------------------------------------------------- embeddings

# Dimensions kept small on purpose. A real text embedding is 768 or 1536 floats; at that width the
# seed file is mostly numbers nobody reads, and every property of the Vectors tab this demonstrates -
# norms, dimension groups, drift, nearest neighbours - is visible at 32. It is comfortably over
# VectorMath.MinDimensions (8) and over JsonDisplay.VectorSummaryOver (24), so the browse grid
# summarises the column rather than printing it, which is the behaviour worth showing.
EMBEDDING_DIMENSIONS = 32


def embedding_for(category, jitter):
    """A unit vector clustered by category, so a nearest-neighbour search returns like with like.

    Category picks a centre; jitter moves the product off it. Two enclosures land near each other and
    far from a cable, which is what makes the similarity search on the Vectors tab mean something
    rather than return an arbitrary twenty.
    """
    centre = random.Random(f"category:{category}")
    base = [centre.gauss(0, 1) for _ in range(EMBEDDING_DIMENSIONS)]

    noise = random.Random(f"product:{jitter}")
    raw = [b + noise.gauss(0, 0.45) for b in base]

    magnitude = math.sqrt(sum(v * v for v in raw)) or 1.0
    return [round(v / magnitude, 6) for v in raw]


# ---------------------------------------------------------------- version chains

def commit(type_name, doc_id, versions, removed=False):
    """
    Writes one document and its history from a list of (body, timestamp) versions, oldest first.

    The envelope row takes the newest body, CreatedAt from v1 and UpdatedAt from the newest version,
    and the newest history row is left open (ValidTo NULL). Deriving all of it from one chain is the
    point: a history sidecar whose newest version disagrees with the row it describes is the failure
    RecordVersion exists to prevent - "the audit trail would be quietly wrong, which is worse than
    having no history tab at all".
    """
    for index, (body, when) in enumerate(versions):
        version = index + 1
        is_last = index == len(versions) - 1
        valid_to = None if is_last else fmt(versions[index + 1][1])

        if is_last and removed:
            operation, data = OP_REMOVED, None
        else:
            operation = OP_INSERTED if version == 1 else OP_UPDATED
            data = dumps(body)

        history.append((doc_id, type_name, version, fmt(when), valid_to,
                        operation, random.choice(ACTORS), data))

    # A removed document keeps its history and loses its row - which is what a tombstone is for.
    if not removed:
        body, when = versions[-1]
        documents.append((doc_id, type_name, dumps(body), fmt(versions[0][1]), fmt(when)))


def timeline(count, earliest_days, latest_days):
    """
    `count` strictly ascending instants, the first roughly `earliest_days` back, the last `latest_days`.

    Sorted after the sub-day jitter is applied, not before: perturbing already-sorted day offsets by
    up to 23h59m independently can swap two adjacent stamps, which puts a version's ValidTo at or
    before its ValidFrom. Strictness matters too - two versions sharing an instant give the closing
    UPDATE an interval of zero length.
    """
    span = max(earliest_days - latest_days, 1)
    stamps = sorted(
        EPOCH - timedelta(days=earliest_days - random.uniform(0, span),
                          hours=random.randint(0, 23),
                          minutes=random.randint(0, 59))
        for _ in range(count)
    )

    for i in range(1, len(stamps)):
        if stamps[i] <= stamps[i - 1]:
            stamps[i] = stamps[i - 1] + timedelta(minutes=random.randint(7, 240))
    return stamps


# ---------------------------------------------------------------- Customer

FIRST = ["Ava", "Noah", "Mia", "Liam", "Zoe", "Ethan", "Nora", "Owen", "Ivy", "Leo",
         "Ruby", "Kai", "Elise", "Milo", "Wren", "Felix", "Sana", "Theo", "Juno", "Arlo"]
LAST = ["Beaulieu", "Okafor", "Lindqvist", "Marchetti", "Nakamura", "Duarte", "Halvorsen",
        "Ferreira", "Kowalski", "Abadi", "Tremblay", "Vasquez", "Bergstrom", "Osei", "Rinaldi"]

TIERS = ["free", "standard", "premium", "enterprise"]

customers = []
for i in range(50):
    first, last = random.choice(FIRST), random.choice(LAST)
    city, region, country, base_lat, base_lng = random.choice(CITIES)
    lat = base_lat + random.uniform(-0.35, 0.35)
    lng = base_lng + random.uniform(-0.35, 0.35)
    doc_id = f"CUST-{1000 + i}"

    # Tier only ever moves up, so the history reads like an account maturing rather than a field
    # being randomised on every version.
    steps = random.choices([1, 2, 3], weights=[45, 35, 20])[0]
    start_tier = random.choices(range(len(TIERS)), weights=[45, 30, 18, 7])[0]
    stamps = timeline(steps, random.randint(120, 1500), random.randint(0, 100))

    # Decided once, so a version diff shows the account maturing rather than every field jittering.
    identity = {
        "id": doc_id,
        "name": f"{first} {last}",
        "email": f"{first.lower()}.{last.lower()}@example.com",
        "signupDate": stamps[0].strftime("%Y-%m-%d"),
        "address": {
            "city": city,
            "region": region,
            "country": country,
            "postalCode": f"{random.randint(10000, 99999)}",
        },
        # Scanned at path "location" - a GeoJSON Point sitting at the top level of the body.
        "location": point(lat, lng),
        "tags": random.sample(["beta", "newsletter", "vip", "churn-risk", "referral", "eu"],
                              k=random.randint(0, 3)),
    }
    # Optional field, so the profiler sees a path present on only some documents.
    manager = f"{random.choice(FIRST)} {random.choice(LAST)}" if random.random() > 0.6 else None
    # Optional geometry, and a second shape class: "serviceArea" is a Polygon while "location" is a
    # Point, so the Geometry tab has more than one path to offer.
    area = (polygon_around(lat, lng, round(random.uniform(0.05, 0.4), 3))
            if random.random() > 0.62 else None)
    active = random.random() > 0.15

    value = round(random.uniform(0, 9_000), 2)
    orders = random.randint(0, 12)
    versions = []
    for step in range(steps):
        body = dict(identity)
        body["tier"] = TIERS[min(start_tier + step, len(TIERS) - 1)]
        body["active"] = active
        body["lifetimeValue"] = value
        body["orderCount"] = orders
        if manager:
            body["accountManager"] = manager
        if area:
            body["serviceArea"] = area

        versions.append((body, stamps[step]))
        value = round(value + random.uniform(500, 14_000), 2)
        orders += random.randint(1, 22)

    commit("Customer", doc_id, versions)
    spatial.append((doc_id, "Customer", *envelope(versions[-1][0]["location"])))
    customers.append((doc_id, versions[-1][0]["tier"], lat, lng))

# ---------------------------------------------------------------- Product

CATEGORIES = ["sensors", "gateways", "cables", "enclosures", "power", "accessories"]
ADJ = ["Compact", "Rugged", "Industrial", "Wireless", "Modular", "Low-Power", "Weatherproof"]
NOUN = ["Relay", "Hub", "Probe", "Bridge", "Node", "Adapter", "Controller", "Beacon"]
CATEGORY_TINT = {
    "sensors": (54, 122, 196), "gateways": (96, 76, 178), "cables": (72, 148, 116),
    "enclosures": (168, 112, 60), "power": (196, 84, 84), "accessories": (108, 108, 124),
}

products = []
for i in range(60):
    category = random.choice(CATEGORIES)
    sku = f"SKU-{category[:3].upper()}-{2000 + i}"
    name = f"{random.choice(ADJ)} {random.choice(NOUN)} {random.choice('IVX')}{random.randint(1, 9)}"
    price = round(random.uniform(4.99, 1899.00), 2)
    currency = random.choice(["CAD", "USD"])

    steps = random.choices([1, 2, 3], weights=[40, 40, 20])[0]
    stamps = timeline(steps, random.randint(60, 900), random.randint(0, 120))
    stock = random.randint(0, 840)

    # A product's identity and physical dimensions do not change when it is repriced or restocked,
    # so they are decided once and the version diff stays about price and stock.
    identity = {
        "id": sku,
        "sku": sku,
        "name": name,
        "category": category,
        "currency": currency,
        "dimensions": {
            "widthMm": random.randint(18, 420),
            "heightMm": random.randint(12, 300),
            "depthMm": random.randint(8, 180),
            "weightG": random.randint(25, 6400),
        },
        "tags": random.sample(["poe", "din-rail", "ip67", "bluetooth", "modbus", "clearance", "new"],
                              k=random.randint(1, 4)),
        "discontinuedOn": None if random.random() > 0.12
        else (EPOCH - timedelta(days=random.randint(1, 700))).strftime("%Y-%m-%d"),

        # Part of the identity rather than of a version: an embedding describes what the product is,
        # so repricing it should not move it in the vector space. That also keeps the History tab's
        # diffs about price and stock instead of about 32 floats that did not change.
        "embedding": embedding_for(category, sku),
    }
    rating = round(random.uniform(2.1, 5.0), 1)
    reviews = random.randint(0, 900)

    versions = []
    for step in range(steps):
        body = dict(identity)
        body["price"] = price
        body["inStock"] = stock > 0
        body["stockLevel"] = stock
        body["rating"] = rating
        body["reviewCount"] = reviews

        versions.append((body, stamps[step]))
        price = round(price * random.uniform(0.88, 1.18), 2)
        stock = max(0, stock + random.randint(-260, 260))
        # Reviews only accumulate, and the average drifts a little as they do.
        reviews += random.randint(0, 180)
        rating = round(min(5.0, max(1.0, rating + random.uniform(-0.3, 0.3))), 1)

    commit("Product", sku, versions)
    final = versions[-1][0]
    products.append((sku, final["price"], currency, name, category))

    created = fmt(versions[0][1])
    updated = fmt(versions[-1][1])

    # Product photography: a real PNG, tinted by category so the grid is not 60 identical squares.
    if random.random() > 0.2:
        tint = CATEGORY_TINT[category]
        shade = random.uniform(0.72, 1.0)

        def pixel(x, y, tint=tint, shade=shade):
            edge = x < 3 or y < 3 or x > 92 or y > 92
            if edge:
                return (232, 232, 238)
            wave = 0.82 + 0.18 * ((x + y) % 24) / 24
            return tuple(min(255, int(c * shade * wave)) for c in tint)

        blobs.append((sku, "Product", "image", png(96, 96, pixel),
                      "image/png", f"{sku.lower()}.png", created, updated))

    if random.random() > 0.75:
        sheet = (
            f"{name}\n{'=' * len(name)}\n\n"
            f"SKU:        {sku}\n"
            f"Category:   {category}\n"
            f"Price:      {final['price']:.2f} {currency}\n"
            f"Dimensions: {final['dimensions']['widthMm']} x {final['dimensions']['heightMm']} x "
            f"{final['dimensions']['depthMm']} mm\n"
            f"Weight:     {final['dimensions']['weightG']} g\n"
        ).encode("utf-8")
        blobs.append((sku, "Product", "datasheet", sheet,
                      "text/plain", f"{sku.lower()}-datasheet.txt", created, updated))

# ---------------------------------------------------------------- Order

STATUS_FLOW = ["pending", "paid", "packed", "shipped", "delivered"]
CARRIERS = ["Purolator", "Canada Post", "UPS", "FedEx", "DHL"]


def build_order(fixed, status):
    """
    One version of an order. Everything except the status is passed in already decided.

    That split is the whole point of the temporal seed: rebuilding the body from scratch per version
    re-rolls every random field, and the version diff then reports that the carrier, tracking number,
    tax and total all changed at the same moment the order shipped. A diff where everything changes
    says nothing about what actually happened, so only the fields a status transition would really
    move are allowed to differ between versions.
    """
    body = {
        "id": fixed["id"],
        "orderNumber": fixed["id"],
        "customerId": fixed["customerId"],
        "customerTier": fixed["customerTier"],
        "status": status,
        "currency": fixed["currency"],
        "subtotal": fixed["subtotal"],
        "shippingCost": fixed["shippingCost"],
        "tax": fixed["tax"],
        "total": fixed["total"],
        "itemCount": fixed["itemCount"],
        "items": fixed["items"],
        # Scanned at path "shipTo.location" - nested one level, well inside the depth-3 walk.
        "shipTo": fixed["shipTo"],
        "expedited": fixed["expedited"],
    }
    # Only shipped-and-beyond orders carry tracking: an optional nested object rather than an
    # optional scalar, which is the case worth having in the sample. The delivery route rides
    # inside it, so a LineString only exists where a shipment does - and the version where it
    # first appears is a clean "added" row in the diff rather than noise.
    if status in ("shipped", "delivered"):
        body["fulfillment"] = fixed["fulfillment"]
    if status in ("cancelled", "refunded"):
        body["cancellationReason"] = fixed["cancellationReason"]
    return body


order_index = 50000
for i in range(93):
    order_id = f"ORD-{order_index + i}"
    customer_id, tier, cust_lat, cust_lng = random.choice(customers)
    city, region, country, base_lat, base_lng = random.choice(CITIES)
    lat = base_lat + random.uniform(-0.25, 0.25)
    lng = base_lng + random.uniform(-0.25, 0.25)
    currency = random.choice(["CAD", "USD"])
    expedited = random.random() > 0.75
    postal = f"{random.randint(10000, 99999)}"
    reason = random.choice(["customer request", "out of stock", "payment failed", "duplicate order"])

    items = []
    for sku, price, _, _, _ in random.sample(products, k=random.randint(1, 5)):
        qty = random.randint(1, 12)
        items.append({"sku": sku, "quantity": qty, "unitPrice": price,
                      "lineTotal": round(price * qty, 2)})

    subtotal = round(sum(x["lineTotal"] for x in items), 2)
    shipping = round(random.uniform(0, 89.5), 2)
    tax = round(subtotal * random.choice([0.05, 0.13, 0.15, 0.0]), 2)

    fixed = {
        "id": order_id,
        "customerId": customer_id,
        "customerTier": tier,
        "currency": currency,
        "subtotal": subtotal,
        "shippingCost": shipping,
        "tax": tax,
        "total": round(subtotal + shipping + tax, 2),
        "itemCount": sum(x["quantity"] for x in items),
        "items": items,
        "shipTo": {
            "city": city,
            "region": region,
            "country": country,
            "postalCode": postal,
            "location": point(lat, lng),
        },
        "expedited": expedited,
        "fulfillment": {
            "carrier": random.choice(CARRIERS),
            "trackingNumber": f"1Z{random.randint(10 ** 9, 10 ** 10 - 1)}",
            "route": route_between(lat, lng, random.randint(2, 5)),
        },
        "cancellationReason": reason,
    }

    # The last three are tombstones: a full version chain ending in Removed, and no envelope row.
    removed = i >= 90

    if removed:
        end = random.choice(["cancelled", "refunded"])
        flow = STATUS_FLOW[:random.randint(1, 3)] + [end]
    else:
        stop = random.choices(range(1, 6), weights=[8, 18, 10, 20, 44])[0]
        flow = STATUS_FLOW[:stop]
        if random.random() > 0.9:
            flow = flow + [random.choice(["cancelled", "refunded"])]

    stamps = timeline(len(flow), random.randint(1, 540), 0)
    versions = [(build_order(fixed, status), stamps[step]) for step, status in enumerate(flow)]

    commit("Order", order_id, versions, removed=removed)

    if not removed:
        spatial.append((order_id, "Order", *envelope(versions[-1][0]["shipTo"]["location"])))

        # An invoice PDF once the money is real. Keeps blobs on more than one type without
        # putting one on every row.
        if flow[-1] in ("shipped", "delivered") and random.random() > 0.55:
            final = versions[-1][0]
            payload = pdf([
                f"Invoice {order_id}",
                "",
                f"Customer: {customer_id} ({tier})",
                f"Ship to:  {city}, {region} {country}",
                "",
                f"Items:    {len(final['items'])}  ({final['itemCount']} units)",
                f"Subtotal: {final['subtotal']:.2f} {currency}",
                f"Shipping: {final['shippingCost']:.2f} {currency}",
                f"Tax:      {final['tax']:.2f} {currency}",
                f"Total:    {final['total']:.2f} {currency}",
            ])
            blobs.append((order_id, "Order", "invoice", payload, "application/pdf",
                          f"{order_id.lower()}-invoice.pdf", fmt(stamps[-1]), fmt(stamps[-1])))

# ---------------------------------------------------------------- self-check
#
# The failure these guard against is silent: a history sidecar that disagrees with the row it
# describes still loads, still renders, and is simply wrong - which is the state RecordVersion
# exists to prevent ("the audit trail would be quietly wrong, which is worse than having no history
# tab at all"). Cheaper to assert here than to notice in the UI.

by_document = {}
for row in history:
    by_document.setdefault((row[0], row[1]), []).append(row)

live = {(doc_id, type_name): data for doc_id, type_name, data, _, _ in documents}

for key, versions in by_document.items():
    versions.sort(key=lambda r: r[2])

    assert [r[2] for r in versions] == list(range(1, len(versions) + 1)), \
        f"{key}: versions are not 1..n"

    open_versions = [r for r in versions if r[4] is None]
    assert len(open_versions) == 1, f"{key}: expected exactly one open version, got {len(open_versions)}"
    assert open_versions[0] is versions[-1], f"{key}: the open version is not the newest"

    for earlier, later in zip(versions, versions[1:]):
        assert earlier[4] == later[3], f"{key} v{earlier[2]}: ValidTo does not meet the next ValidFrom"
        assert earlier[3] < earlier[4], f"{key} v{earlier[2]}: interval ends at or before it starts"

    newest = versions[-1]
    if newest[5] == OP_REMOVED:
        assert newest[7] is None, f"{key}: a tombstone must carry no body"
        assert key not in live, f"{key}: tombstoned but still has an envelope row"
    else:
        assert key in live, f"{key}: has history but no envelope row"
        assert newest[7] == live[key], f"{key}: the open version disagrees with the envelope row"

for doc_id, type_name, _, created, updated in documents:
    assert (doc_id, type_name) in by_document, f"{doc_id}: envelope row with no history"
    versions = by_document[(doc_id, type_name)]
    assert created == versions[0][3], f"{doc_id}: CreatedAt is not v1's ValidFrom"
    assert updated == versions[-1][3], f"{doc_id}: UpdatedAt is not the open version's ValidFrom"

for doc_id, type_name, key, payload, content_type, file_name, _, _ in blobs:
    assert (doc_id, type_name) in live, f"{doc_id}/{key}: blob on a document that does not exist"
    assert len(payload) > 0, f"{doc_id}/{key}: empty payload"

for doc_id, type_name, data, _, _ in documents:
    if type_name != "Product":
        continue

    values = json.loads(data)["embedding"]
    assert len(values) == EMBEDDING_DIMENSIONS, f"{doc_id}: embedding is {len(values)} wide"
    norm = math.sqrt(sum(v * v for v in values))
    assert abs(norm - 1.0) < 0.001, f"{doc_id}: embedding is not unit length ({norm})"

for doc_id, type_name, min_lat, max_lat, min_lng, max_lng in spatial:
    assert (doc_id, type_name) in live, f"{doc_id}: spatial row on a document that does not exist"
    assert -90 <= min_lat <= max_lat <= 90, f"{doc_id}: latitude bounds out of range"
    assert -180 <= min_lng <= max_lng <= 180, f"{doc_id}: longitude bounds out of range"


# ---------------------------------------------------------------- emit

def q(text):
    return "'" + text.replace("'", "''") + "'"


def val(text):
    return "NULL" if text is None else q(text)


print("-- Generated by seed-demo.py - do not edit by hand.")
print("-- Seed data for the ShinyDocDbMyAdmin public playground.")
print("--")
print("-- Schema matches SqliteDatabaseProvider: BuildCreateTableSql, BuildCreateHistoryTableSql,")
print("-- BuildCreateBlobTableSql and BuildCreateSpatialTablesSql, for the default table name")
print('-- (DocumentStoreOptions.TableName = "documents").')
print("--")
print(f"-- {len(documents)} documents, {len(history)} versions, {len(blobs)} blobs, "
      f"{len(spatial)} spatial rows.")
print()
print("PRAGMA journal_mode=WAL;")
print()

print("""CREATE TABLE IF NOT EXISTS "documents" (
    Id TEXT NOT NULL,
    TypeName TEXT NOT NULL,
    Data TEXT NOT NULL,
    CreatedAt TEXT NOT NULL,
    UpdatedAt TEXT NOT NULL,
    PRIMARY KEY (Id, TypeName)
);
CREATE INDEX IF NOT EXISTS idx_documents_typename ON "documents" (TypeName);

CREATE TABLE IF NOT EXISTS "documents_history" (
    Id TEXT NOT NULL,
    TypeName TEXT NOT NULL,
    Version INTEGER NOT NULL,
    ValidFrom TEXT NOT NULL,
    ValidTo TEXT NULL,
    Operation TEXT NOT NULL,
    Actor TEXT NULL,
    Data TEXT NULL,
    TenantId TEXT NULL,
    PRIMARY KEY (Id, TypeName, Version)
);
CREATE INDEX IF NOT EXISTS idx_documents_history_range
    ON "documents_history" (TypeName, ValidFrom, ValidTo);
CREATE INDEX IF NOT EXISTS idx_documents_history_actor
    ON "documents_history" (TypeName, Actor);

CREATE TABLE IF NOT EXISTS "documents_blobs" (
    Id TEXT NOT NULL,
    TypeName TEXT NOT NULL,
    BlobKey TEXT NOT NULL,
    Data BLOB NOT NULL,
    Length INTEGER NOT NULL,
    ContentType TEXT NULL,
    FileName TEXT NULL,
    Hash TEXT NULL,
    CreatedAt TEXT NOT NULL,
    UpdatedAt TEXT NOT NULL,
    PRIMARY KEY (Id, TypeName, BlobKey)
);

CREATE TABLE IF NOT EXISTS documents_spatial_map (
    rowid INTEGER PRIMARY KEY AUTOINCREMENT,
    docId TEXT NOT NULL,
    typeName TEXT NOT NULL,
    UNIQUE(docId, typeName)
);
CREATE VIRTUAL TABLE IF NOT EXISTS documents_spatial USING rtree(
    id,
    minLat, maxLat,
    minLng, maxLng
);

-- Full text over Product, exactly as SqliteDatabaseProvider.BuildCreateFullTextSql writes it for
-- MapFullTextProperty(x => x.Name, x => x.Category, x => x.Tags). Created before the inserts below,
-- so the triggers fill the index as the documents land - the same way it fills when the library
-- writes one. FTS5 ships the Porter stemmer for English only, hence the tokenizer.
CREATE VIRTUAL TABLE IF NOT EXISTS documents_fts USING fts5(docId UNINDEXED, typeName UNINDEXED, text, tokenize='porter unicode61');

CREATE TRIGGER IF NOT EXISTS documents_fts_ai_Product AFTER INSERT ON "documents"
WHEN new.TypeName = 'Product' BEGIN
    INSERT INTO documents_fts(rowid, docId, typeName, text) VALUES (new.rowid, new.Id, new.TypeName, COALESCE(json_extract(new.Data, '$.name'), '') || ' ' || COALESCE(json_extract(new.Data, '$.category'), '') || ' ' || COALESCE(json_extract(new.Data, '$.tags'), ''));
END;

CREATE TRIGGER IF NOT EXISTS documents_fts_ad_Product AFTER DELETE ON "documents"
WHEN old.TypeName = 'Product' BEGIN
    DELETE FROM documents_fts WHERE rowid = old.rowid;
END;

CREATE TRIGGER IF NOT EXISTS documents_fts_au_Product AFTER UPDATE ON "documents"
WHEN new.TypeName = 'Product' BEGIN
    DELETE FROM documents_fts WHERE rowid = old.rowid;
    INSERT INTO documents_fts(rowid, docId, typeName, text) VALUES (new.rowid, new.Id, new.TypeName, COALESCE(json_extract(new.Data, '$.name'), '') || ' ' || COALESCE(json_extract(new.Data, '$.category'), '') || ' ' || COALESCE(json_extract(new.Data, '$.tags'), ''));
END;""")
print()
print("BEGIN TRANSACTION;")
print()

print("-- documents")
for doc_id, type_name, data, created, updated in documents:
    print(f'INSERT INTO "documents" (Id, TypeName, Data, CreatedAt, UpdatedAt) '
          f"VALUES ({q(doc_id)}, {q(type_name)}, {q(data)}, {q(created)}, {q(updated)});")

print()
print("-- documents_history")
for doc_id, type_name, version, valid_from, valid_to, operation, actor, data in history:
    print(f'INSERT INTO "documents_history" '
          f"(Id, TypeName, Version, ValidFrom, ValidTo, Operation, Actor, Data) "
          f"VALUES ({q(doc_id)}, {q(type_name)}, {version}, {q(valid_from)}, {val(valid_to)}, "
          f"{q(operation)}, {q(actor)}, {val(data)});")

print()
print("-- documents_blobs (Hash is the lowercase hex SHA-256 the library computes; see DocumentBlob)")
for doc_id, type_name, key, payload, content_type, file_name, created, updated in blobs:
    digest = hashlib.sha256(payload).hexdigest()
    print(f'INSERT INTO "documents_blobs" '
          f"(Id, TypeName, BlobKey, Data, Length, ContentType, FileName, Hash, CreatedAt, UpdatedAt) "
          f"VALUES ({q(doc_id)}, {q(type_name)}, {q(key)}, X'{payload.hex()}', {len(payload)}, "
          f"{q(content_type)}, {q(file_name)}, {q(digest)}, {q(created)}, {q(updated)});")

print()
print("-- documents_spatial_map / documents_spatial (bounding boxes for the GeoJSON in the bodies)")
for doc_id, type_name, data, _, _ in documents:
    if type_name != "Product":
        continue

    values = json.loads(data)["embedding"]
    assert len(values) == EMBEDDING_DIMENSIONS, f"{doc_id}: embedding is {len(values)} wide"
    norm = math.sqrt(sum(v * v for v in values))
    assert abs(norm - 1.0) < 0.001, f"{doc_id}: embedding is not unit length ({norm})"

for doc_id, type_name, min_lat, max_lat, min_lng, max_lng in spatial:
    print(f"INSERT INTO documents_spatial_map (docId, typeName) "
          f"VALUES ({q(doc_id)}, {q(type_name)});")
    print(f"INSERT INTO documents_spatial (id, minLat, maxLat, minLng, maxLng) VALUES ("
          f"(SELECT rowid FROM documents_spatial_map "
          f"WHERE docId = {q(doc_id)} AND typeName = {q(type_name)}), "
          f"{min_lat}, {max_lat}, {min_lng}, {max_lng});")

print()
print("COMMIT;")
