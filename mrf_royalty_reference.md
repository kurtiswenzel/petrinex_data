# Alberta Modernized Royalty Framework (MRF) -- Calculation Reference

For wells spud on or after January 1, 2017. Source: [Alberta Energy MRF Formulas](https://open.alberta.ca/publications/modernized-royalty-framework-formulas).

## Overview

Every well pays a **5% minimum royalty** until cumulative revenue reaches **C\*** (a proxy for drilling and completion costs). After C\*, the royalty rate is:

```
R% = rp (price component) + rq (quantity adjustment)
```

R% is bounded by product-specific minimums and maximums.

## C\* -- Drilling Cost Proxy

C\* determines when a well transitions from the flat 5% royalty to the formula-based rate.

### For wells with TVD_MAX <= 2,000 m

```
C* = ACCI * ((1170 * (TVD_MAX - 249)) + (Y * 800 * TLL) + (0.6 * TVD_AVG * TPPe))
```

### For wells with TVD_MAX > 2,000 m

```
C* = ACCI * ((1170 * (TVD_MAX - 249)) + (3120 * (TVD_MAX - 2000)) + (Y * 800 * TLL) + (0.6 * TVD_AVG * TPPe))
```

### Parameters

| Parameter | Description |
|-----------|-------------|
| ACCI | Alberta Capital Cost Index (published quarterly by Alberta Energy) |
| TVD_MAX | Deepest True Vertical Depth (m) |
| TVD_AVG | Average TVD across all legs (m), non-reported legs = 0 |
| TLL | Total Lateral Length (m) |
| TPPe | Total Equivalent Proppant Placed (tonnes) |
| TMD | Total Measured Depth (m), combined length of all legs |
| Y | Multi-leg cost adjustment factor |

### Y Factor

```
if TMD / TVD_AVG < 10:
    Y = 1
else:
    Y = max(0.24, 1.39 - 0.04 * (TMD / TVD_AVG))
```

### Revenue for C\* Comparison

Cumulative revenue = sum of (volume * par price) for each product. Oil uses produced volumes; gas and by-products use allocated volumes.

## Royalty Rate Formulas by Product

All formulas follow: `R% = rp + rq`, applied after cumulative revenue exceeds C\*.

### Conventional Oil, Pentane Plus, and Field Condensate

- **Minimum:** 5%, **Maximum:** 40%
- Price unit: C$/m3

**Price Component (rp):**

| Price (PP) | rp |
|------------|-----|
| PP <= 251.70 | 10% |
| 251.70 < PP <= 409.02 | ((PP - 251.70) * 0.00071 + 0.10) * 100 |
| 409.02 < PP <= 723.64 | ((PP - 409.02) * 0.00039 + 0.21170) * 100 |
| PP > 723.64 | ((PP - 723.64) * 0.00020 + 0.33440) * 100 |

**Quantity Adjustment (rq):**

- Maturity threshold: 194.0 m3e/month
- If Q >= 194.0: rq = 0%
- If Q < 194.0: rq = (Q - 194.0) * 0.001350 * 100 (always negative or zero)

```python
def oil_royalty_rate(price_per_m3, oil_equiv_m3e_per_month):
    """MRF royalty rate for oil/pentane+/condensate."""
    if price_per_m3 <= 251.70:
        rp = 10.0
    elif price_per_m3 <= 409.02:
        rp = ((price_per_m3 - 251.70) * 0.00071 + 0.10) * 100
    elif price_per_m3 <= 723.64:
        rp = ((price_per_m3 - 409.02) * 0.00039 + 0.21170) * 100
    else:
        rp = ((price_per_m3 - 723.64) * 0.00020 + 0.33440) * 100

    if oil_equiv_m3e_per_month >= 194.0:
        rq = 0.0
    else:
        rq = (oil_equiv_m3e_per_month - 194.0) * 0.001350 * 100

    return max(5.0, min(40.0, rp + rq))
```

### Natural Gas (Methane) and Ethane

- **Minimum:** 5%, **Maximum:** 36%
- Price unit: C$/GJ

**Price Component (rp):**

| Price (PP) | rp |
|------------|-----|
| PP <= 2.40 | 5% |
| 2.40 < PP <= 3.00 | ((PP - 2.40) * 0.06 + 0.05) * 100 |
| 3.00 < PP <= 6.75 | ((PP - 3.00) * 0.0425 + 0.086) * 100 |
| PP > 6.75 | ((PP - 6.75) * 0.0225 + 0.24538) * 100 |

**Quantity Adjustment (rq):**

- Maturity threshold: 345.5 e3m3e/month
- If Q >= 345.5: rq = 0%
- If Q < 345.5: rq = (Q - 345.5) * 0.0004937 * 100

```python
def gas_royalty_rate(price_per_gj, gas_equiv_e3m3e_per_month):
    """MRF royalty rate for natural gas and ethane."""
    if price_per_gj <= 2.40:
        rp = 5.0
    elif price_per_gj <= 3.00:
        rp = ((price_per_gj - 2.40) * 0.06 + 0.05) * 100
    elif price_per_gj <= 6.75:
        rp = ((price_per_gj - 3.00) * 0.0425 + 0.086) * 100
    else:
        rp = ((price_per_gj - 6.75) * 0.0225 + 0.24538) * 100

    if gas_equiv_e3m3e_per_month >= 345.5:
        rq = 0.0
    else:
        rq = (gas_equiv_e3m3e_per_month - 345.5) * 0.0004937 * 100

    return max(5.0, min(36.0, rp + rq))
```

### Propane (extracted and in-stream)

- **Minimum:** 5%, **Maximum:** 36%
- Price unit: C$/m3

**Price Component (rp):**

| Price (PP) | rp |
|------------|-----|
| PP <= 88.10 | 10% |
| 88.10 < PP <= 143.16 | ((PP - 88.10) * 0.00202 + 0.10) * 100 |
| 143.16 < PP <= 253.28 | ((PP - 143.16) * 0.00111 + 0.21122) * 100 |
| PP > 253.28 | ((PP - 253.28) * 0.00059 + 0.33347) * 100 |

**Quantity Adjustment (rq):** Same as oil -- threshold 194.0 m3e/month, slope 0.001350.

```python
def propane_royalty_rate(price_per_m3, oil_equiv_m3e_per_month):
    """MRF royalty rate for propane."""
    if price_per_m3 <= 88.10:
        rp = 10.0
    elif price_per_m3 <= 143.16:
        rp = ((price_per_m3 - 88.10) * 0.00202 + 0.10) * 100
    elif price_per_m3 <= 253.28:
        rp = ((price_per_m3 - 143.16) * 0.00111 + 0.21122) * 100
    else:
        rp = ((price_per_m3 - 253.28) * 0.00059 + 0.33347) * 100

    if oil_equiv_m3e_per_month >= 194.0:
        rq = 0.0
    else:
        rq = (oil_equiv_m3e_per_month - 194.0) * 0.001350 * 100

    return max(5.0, min(36.0, rp + rq))
```

### Butane (extracted and in-stream)

- **Minimum:** 5%, **Maximum:** 36%
- Price unit: C$/m3

**Price Component (rp):**

| Price (PP) | rp |
|------------|-----|
| PP <= 176.19 | 10% |
| 176.19 < PP <= 286.31 | ((PP - 176.19) * 0.00101 + 0.10) * 100 |
| 286.31 < PP <= 506.55 | ((PP - 286.31) * 0.00055 + 0.21122) * 100 |
| PP > 506.55 | ((PP - 506.55) * 0.00031 + 0.33235) * 100 |

**Quantity Adjustment (rq):** Same as oil -- threshold 194.0 m3e/month, slope 0.001350.

```python
def butane_royalty_rate(price_per_m3, oil_equiv_m3e_per_month):
    """MRF royalty rate for butane."""
    if price_per_m3 <= 176.19:
        rp = 10.0
    elif price_per_m3 <= 286.31:
        rp = ((price_per_m3 - 176.19) * 0.00101 + 0.10) * 100
    elif price_per_m3 <= 506.55:
        rp = ((price_per_m3 - 286.31) * 0.00055 + 0.21122) * 100
    else:
        rp = ((price_per_m3 - 506.55) * 0.00031 + 0.33235) * 100

    if oil_equiv_m3e_per_month >= 194.0:
        rq = 0.0
    else:
        rq = (oil_equiv_m3e_per_month - 194.0) * 0.001350 * 100

    return max(5.0, min(36.0, rp + rq))
```

## Oil Equivalent Volume Calculation

The quantity adjustment uses a well's total oil-equivalent or gas-equivalent production. To combine products into oil equivalent (m3e/month):

```python
def oil_equivalent_m3e(oil_m3, condensate_m3, pentane_m3, propane_m3, butane_m3):
    """Convert well production to oil equivalent m3e for quantity adjustment."""
    return (
        oil_m3                    # 1:1
        + condensate_m3           # 1:1
        + pentane_m3              # 1:1
        + propane_m3 * 0.6817     # propane equivalency
        + butane_m3 * 0.7908      # butane equivalency
    )
```

For gas equivalent (e3m3e/month):

```python
def gas_equivalent_e3m3e(gas_e3m3, ethane_e3m3, oil_m3, condensate_m3,
                          pentane_m3, propane_m3, butane_m3):
    """Convert well production to gas equivalent e3m3e for quantity adjustment."""
    return (
        gas_e3m3                  # 1:1
        + ethane_e3m3             # 1:1
        + oil_m3 * 5.6146         # oil to gas equivalent (6:1)
        + condensate_m3 * 5.6146
        + pentane_m3 * 5.6146
        + propane_m3 * 0.003829
        + butane_m3 * 0.004441
    )
```

## Applying MRF to Petrinex Data

The Petrinex `ngl_volumes` table provides well-level monthly production needed for royalty calculations. The `market_prices` table provides benchmark prices, but par prices for royalty calculations are published separately by Alberta Energy.

### Example: Estimate oil royalty rate for a well-month

```python
from pyspark.sql import functions as F

ngl = spark.table("shm.petrinex.ngl_volumes")
prices = spark.table("shm.petrinex.market_prices")

well_monthly = (
    ngl
    .filter(F.col("WellID").isNotNull())
    .withColumn("oil_m3", F.col("OilProduction").cast("double"))
    .withColumn("cond_m3", F.col("CondensateProduction").cast("double"))
    .withColumn("pentane_m3",
        F.col("PentaneMixVolume").cast("double") + F.col("PentaneSpecVolume").cast("double"))
    .withColumn("propane_m3",
        F.col("PropaneMixVolume").cast("double") + F.col("PropaneSpecVolume").cast("double"))
    .withColumn("butane_m3",
        F.col("ButaneMixVolume").cast("double") + F.col("ButaneSpecVolume").cast("double"))
    .withColumn("oil_equiv_m3e",
        F.col("oil_m3")
        + F.col("cond_m3")
        + F.col("pentane_m3")
        + F.col("propane_m3") * 0.6817
        + F.col("butane_m3") * 0.7908)
)
```

## Reference Links

- [MRF Formula Documents (Alberta Open Data)](https://open.alberta.ca/publications/modernized-royalty-framework-formulas)
- [Oil/Pentane+/Condensate Formula (PDF)](https://open.alberta.ca/dataset/db4b53fc-092b-4eba-9624-0d71f7ed76a9/resource/77bb6095-9841-42f0-9de6-3f80fc18d286/download/mrfoil.pdf)
- [Natural Gas and Ethane Formula (PDF)](https://open.alberta.ca/dataset/db4b53fc-092b-4eba-9624-0d71f7ed76a9/resource/02cfa93e-0d8d-4a9d-bc3a-f8e83054c375/download/mrfgas.pdf)
- [Propane Formula (PDF)](https://open.alberta.ca/dataset/db4b53fc-092b-4eba-9624-0d71f7ed76a9/resource/076953b6-b496-43b1-982d-52c0c573ac54/download/mrfpropane.pdf)
- [Butane Formula (PDF)](https://open.alberta.ca/dataset/db4b53fc-092b-4eba-9624-0d71f7ed76a9/resource/1da51b5f-7714-436e-a762-9097aab091f9/download/mrfbutane.pdf)
- [C* Formula (PDF)](https://open.alberta.ca/dataset/db4b53fc-092b-4eba-9624-0d71f7ed76a9/resource/b0800f4d-85d6-4c39-b366-ed332be82214/download/mrfcstar.pdf)
