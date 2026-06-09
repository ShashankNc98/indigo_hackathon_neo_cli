# Aviation Loyalty Plugin — Neo DAG Template Kit

A portable, airline-agnostic set of Neo DAG templates, rule patterns, and configuration schemas for building flight-accrual and fraud-detection loyalty dataflows on Capillary Neo.

## Repository Layout

```
aviation-loyalty-plugin/
├── README.md                              ← this file
├── defaults/
│   └── EXAMPLE.config.json               ← blank starter config (copy & fill)
├── schema/
│   └── plugin.schema.json                ← JSON Schema for config validation
├── rule-compiler/
│   ├── rule-library.json                 ← 6 pre-built aviation rule patterns
│   └── field-registry.json               ← canonical field names for AI authoring
└── templates/
    ├── PreFlown.template.dag.ts           ← booking accrual (12 placeholders)
    ├── PostFlown.template.dag.ts          ← post-flight accrual (13 placeholders)
    ├── CancelPNR-Orchestrator.template.dag.ts
    ├── CancelPNR-CouponReversal.template.dag.ts
    ├── CancelPNR-PersistPNR.template.dag.ts
    ├── UpdateCouponRedemption.template.dag.ts
    ├── UpdatePointsRedemption.template.dag.ts
    └── BlockFraudCustomers.template.dag.ts ← fraud detection (8 placeholders)
```

## Quick Start

### 1. Copy and fill the config

```bash
cp defaults/EXAMPLE.config.json my-airline.config.json
# edit my-airline.config.json with your airline-specific values
```

### 2. Validate the config

```bash
neo validate --json my-airline.config.json --schema schema/plugin.schema.json
```

### 3. Stamp templates

Replace every `{{PLACEHOLDER}}` in a template with values from your config:

```bash
# example using envsubst (bash)
export DAG_URL="postFlownBookings"
export LOYALTY_CURRENCY_NAME="BluChips"
# ... (see EXAMPLE.config.json for full list)
envsubst < templates/PostFlown.template.dag.ts > PostFlown.dag.ts
```

Or use the rule compiler to auto-stamp from a config file:

```bash
node rule-compiler/stamp.js --config my-airline.config.json --template templates/PostFlown.template.dag.ts
```

### 4. Push to Neo

```bash
neo validate PostFlown.dag.ts
neo push PostFlown.dag.ts
```

## Placeholder Convention

All placeholders follow the `{{UPPER_SNAKE_CASE}}` pattern.  
Each template lists its placeholders at the top as a comment block.

## Templates

| Template | Description | Placeholders |
|---|---|---|
| `PreFlown` | Accrual on booking / pre-departure | 12 |
| `PostFlown` | Accrual after flight is flown | 13 |
| `CancelPNR-Orchestrator` | Entry point for PNR cancellation | 6 |
| `CancelPNR-CouponReversal` | Reverses coupon/transaction on cancel | 5 |
| `CancelPNR-PersistPNR` | Updates Mongo on cancel | 4 |
| `UpdateCouponRedemption` | Syncs coupon redemption state | 5 |
| `UpdatePointsRedemption` | Syncs points redemption state | 5 |
| `BlockFraudCustomers` | Event-driven fraud detection + alerting | 8 |

## Rule Compiler

`rule-compiler/rule-library.json` contains 6 pre-built patterns:
1. **AliasMatcher** — FFN/loyalty-ID to customer alias resolution
2. **DuplicatePNRGuard** — rejects duplicate bill numbers
3. **StaffTravelFilter** — excludes staff/duty-travel PNRs
4. **FraudEmailAlert** — multi-condition fraud email trigger
5. **BillingDateWindow** — configurable lookback window check
6. **BoardingStatusGate** — boarding-status-aware accrual routing

`rule-compiler/field-registry.json` lists all supported extended/custom fields for use when prompting an AI to author new DAG blocks.
