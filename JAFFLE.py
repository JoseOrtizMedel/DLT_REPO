import dlt
import requests
import time
import os

BASE_URL = "https://jaffle-shop.scalevector.ai/api/v1"


# ─────────────────────────────────────────────
# NAIVE
# ─────────────────────────────────────────────

def fetch_rows(endpoint: str, page_size: int = 100):
    page = 1
    while True:
        items = requests.get(
            BASE_URL + endpoint,
            params={"page": page, "page_size": page_size}
        ).json()
        if not items:
            break
        for item in items:
            yield item
        if len(items) < page_size:
            break
        page += 1


@dlt.resource(name="customers", write_disposition="replace")
def naive_customers():
    yield from fetch_rows("/customers")


@dlt.resource(name="orders", write_disposition="replace")
def naive_orders():
    yield from fetch_rows("/orders")


@dlt.resource(name="products", write_disposition="replace")
def naive_products():
    yield from fetch_rows("/products")


pipeline_naive = dlt.pipeline(
    pipeline_name="jaffle_naive",
    destination="duckdb",
    dataset_name="jaffle_naive",
    dev_mode=True,
)

print("Ejecutando NAIVE...")
t0 = time.perf_counter()
pipeline_naive.run([naive_customers(), naive_orders(), naive_products()])
t_naive = time.perf_counter() - t0

trace_naive = pipeline_naive.last_trace
steps_naive = {s.step: (s.finished_at - s.started_at).total_seconds() for s in trace_naive.steps}
print(f"Tiempo total NAIVE: {t_naive:.2f}s")


# ─────────────────────────────────────────────
# OPTIMIZADO
# ─────────────────────────────────────────────

os.environ["EXTRACT__WORKERS"] = "4"
os.environ["NORMALIZE__WORKERS"] = "4"
os.environ["LOAD__WORKERS"] = "4"
os.environ["DATA_WRITER__BUFFER_MAX_ITEMS"] = "5000"
os.environ["DATA_WRITER__FILE_MAX_ITEMS"] = "10000"


def fetch_chunks(endpoint: str, page_size: int = 100):
    page = 1
    while True:
        items = requests.get(
            BASE_URL + endpoint,
            params={"page": page, "page_size": page_size}
        ).json()
        if not items:
            break
        yield items
        if len(items) < page_size:
            break
        page += 1


@dlt.resource(name="customers", write_disposition="replace", parallelized=True)
def opt_customers():
    yield from fetch_chunks("/customers")


@dlt.resource(name="orders", write_disposition="replace", parallelized=True)
def opt_orders():
    yield from fetch_chunks("/orders")


@dlt.resource(name="products", write_disposition="replace", parallelized=True)
def opt_products():
    yield from fetch_chunks("/products")


@dlt.source
def jaffle_shop():
    return (opt_customers, opt_orders, opt_products)


pipeline_opt = dlt.pipeline(
    pipeline_name="jaffle_optimized",
    destination="duckdb",
    dataset_name="jaffle_optimized",
    dev_mode=True,
)

print("Ejecutando OPTIMIZADO...")
t0 = time.perf_counter()
pipeline_opt.run(jaffle_shop())
t_opt = time.perf_counter() - t0

trace_opt = pipeline_opt.last_trace
steps_opt = {s.step: (s.finished_at - s.started_at).total_seconds() for s in trace_opt.steps}
print(f"Tiempo total OPTIMIZADO: {t_opt:.2f}s")


# ─────────────────────────────────────────────
# COMPARATIVA
# ─────────────────────────────────────────────

print("\n" + "=" * 55)
print(f"{'Etapa':<15} {'Naive (s)':>12} {'Opt (s)':>12} {'Speedup':>10}")
print("-" * 55)
for step in ["extract", "normalize", "load"]:
    t_n = steps_naive.get(step, 0)
    t_o = steps_opt.get(step, 0)
    speedup = (t_n / t_o) if t_o > 0 else float("inf")
    print(f"{step:<15} {t_n:>12.2f} {t_o:>12.2f} {speedup:>9.2f}x")
print("-" * 55)
print(f"{'TOTAL':<15} {t_naive:>12.2f} {t_opt:>12.2f} {t_naive/t_opt:>9.2f}x")
@JoseOrtizMedel
Comment
