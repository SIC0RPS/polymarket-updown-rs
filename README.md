# polymarket-updown-rs

Poly-updown is an experimental Rust trader for Polymarket crypto UP/DOWN
markets, with a React dashboard, local data capture, retained-data backtesting
and CMA-ES parameter optimization. It supports BTC, ETH, SOL and XRP.

## Final handoff

I developed this project privately and am now sharing it publicly so others
can learn from it or continue in a fork. This is my first and final commit here.
I've finished working on it and won't be providing further updates or support.

## Why this is public

I spent about six months building and experimenting with this system. My
development observations included millisecond-scale local signal-to-reaction
times and high oracle prediction win rates in some sessions. Those observations
are not independently verified benchmarks, and this release includes no
historical dataset with which to reproduce them.

**The strategy did not achieve reliable profitability in my testing.** Correct
predictions and fast reactions did not prevent larger losses from outweighing
many smaller wins. Prediction accuracy, execution speed and trading profitability
are separate results.

I am sharing the code as a base for learning about Polymarket trading systems
and experimenting with better strategies. No profitable strategy or future
performance is promised.

## How the system works

1. **Market data:** ingest Polymarket UP/DOWN order books and reference-market
   data, including Binance inputs and Chainlink observations.
2. **Oracle:** combine the existing forecast, market information and qualified
   learned contributions into probabilities and an emitted directional signal.
3. **Strategy and execution:** evaluate entry or exit instructions against the
   current book, sizing, liquidity and risk constraints, then submit orders.
4. **Accounting:** confirmed receipts feed the shared market-entry ledger and
   reducer. Live trading, retained-capture backtests and optimization use the
   same inventory, basis, fees, settlement and realized-PnL accounting.
5. **Dashboard:** inspect orders, positions, settlement, wallet equity, realized
   and unrealized PnL, and exit attribution.

There are two acquisition modes, selected by
`strategy.maker_rebate_mm.oracle_buy_and_hold` in `config/main.toml`:

| Mode | Entry and position lifecycle |
|---|---|
| BUY-only (`true`) | Wait for the existing First activation, then buy the oracle-selected outcome directly. |
| CTF (`false`) | Split collateral into both outcomes; First can sell a leg, leaving the remaining holding. |

For a remaining holding, **DeadCat** attempts loss recovery on a qualifying
opposing oracle signal, subject to recovery and execution requirements.
**ProfitNow** requires positive whole-cohort net profit that meets the existing
expected-settlement-profit comparison. Otherwise the position continues through
the hold-to-resolution and settlement lifecycle. These rules do not guarantee
loss protection or profitable execution.

Application telemetry distinguishes market-frame receipt to decision, receipt
to submission invocation, and receipt to canonical receipt admission.
Millisecond-scale local reaction does not establish exchange arrival, fill time
or blockchain confirmation time.

### Set the mode in main.toml

Edit the existing `[strategy.maker_rebate_mm]` section in
[`config/main.toml`](config/main.toml) before starting the trader:

```toml
[strategy.maker_rebate_mm]
oracle_buy_and_hold = true # BUY-only; set false for CTF
```

Use `true` for BUY-only and `false` for CTF. The supplied configuration selects
BUY-only. Change the existing key rather than adding a second copy of the
section. By default, live trading, backtesting and local optimization select
the corresponding mode's parameter files automatically.

### Included base parameter profiles

All eight per-symbol profiles are included below, covering both modes. They
supply the project's existing development parameter defaults as starting
baselines; users do not have to run an optimization campaign to obtain initial
settings. These files are not evidence of profitability or a recommendation
to trade with those settings.

| Symbol | CTF base profile | BUY-only base profile |
|---|---|---|
| BTC | [BTC.optimization.yml](BTC.optimization.yml) | [BTC.optimization.buy.yml](BTC.optimization.buy.yml) |
| ETH | [ETH.optimization.yml](ETH.optimization.yml) | [ETH.optimization.buy.yml](ETH.optimization.buy.yml) |
| SOL | [SOL.optimization.yml](SOL.optimization.yml) | [SOL.optimization.buy.yml](SOL.optimization.buy.yml) |
| XRP | [XRP.optimization.yml](XRP.optimization.yml) | [XRP.optimization.buy.yml](XRP.optimization.buy.yml) |

Each symbol file contains its allocation limit and parameter `default` values.
Keep both sets in the repository; no copying or renaming is needed to switch
modes. The root [`optimization.yml`](optimization.yml) and
[`optimization.buy.yml`](optimization.buy.yml) define the CTF and BUY-only
search specifications respectively, rather than replacing the per-symbol
starting profiles. Private trained learner snapshots are not bundled.

## Oracle learning and prediction scores

The oracle contains three learning components: an L2-regularized microstructure
logistic model, a market-probability beta calibrator and a final-model beta
calibrator. They learn from released outcome labels. Learned contributions are
qualified against their reference using prediction-loss and sample-support
checks; learning does not guarantee that each new fit will improve performance.

**Brier score is a probability-error score, not a learner type or a win rate.**
For an UP probability `p` and observed outcome `y` (UP = 1, DOWN = 0), its
per-observation loss is `(p - y)^2`; lower is better. Prediction reports also
measure log loss, directional accuracy and signal availability. The prediction
aggregator averages observations within each market, then gives represented
markets equal weight.

Directional accuracy is different from the percentage of profitable trades.
Purchase price, fees, liquidity, partial execution and exit decisions all
affect realized results. No fixed prediction win rate is claimed for this release.

Live and replay use the same learning owners and snapshot format. Captures
retain an initial learning snapshot; replay can export its final snapshot.
Live continuation state and optimizer-selected learning snapshots live under
`data/oracle_learning/`. Live learning updates models as labels arrive;
it does not automatically run CMA-ES or replace the strategy parameter profiles.

## Build and run locally

Use a Git checkout, the Rust toolchain pinned in `rust-toolchain.toml`, a native
C/C++ build toolchain, and Node.js/npm.

```sh
npm --prefix frontend ci
make build
npm --prefix frontend run build
```

Configure `config/main.toml` and the symbol profiles. Copy `.env.example` to
`../.env` and supply your own signing key. Copy
`config/pia-socks.env.example` to `config/pia-socks.env` and supply your own
required proxy credentials. Keep filled credential files private.

```sh
make dev
# Dashboard: http://127.0.0.1:5188

# Stop the trader and frontend from another terminal:
make dev-stop
```

The supplied configuration starts paused with an empty wallet address.
This is a live-capable trader: unpausing a configured installation can submit
real orders.

## Capture and replay

With `[capture].enabled = true`, live startup records a capture under
`data/captures/`. Bundles retain configuration and optimizer snapshots,
initial learner state, market inputs, strategy consumption, execution
observations and receipt facts for offline replay.

The supplied capture settings are a one-hour recording limit, a 30-minute
minimum-save setting and retention of up to 12 regular valid finalized captures.
Older qualifying captures are deleted by retention. Incomplete captures are not
eligible for replay. Reaching the recording limit stops that capture;
the live trader can continue running.

No captures, logs, private datasets or trained learner snapshots are bundled.
Record your own usable captures before running a backtest or optimization.

```sh
make backtest-strategy

# Select a particular capture instead of the default latest replay-ready one:
BACKTEST_STRATEGY_CAPTURE="data/captures/YOUR_CAPTURE" make backtest-strategy
```

Ordinary backtests use the current installed configuration and mode-specific
symbol profiles against recorded inputs. Replay does not fetch replacement
market facts. Preparation excludes unavailable symbols before the participating
scope is frozen; invalid frozen inputs still fail validation.

Native reports are written beneath the capture at
`backtest_reports/matrix/backtest_strategy_report.{md,json,txt}`.

## Local optimization

```sh
make optimize

# Fix the selection capture A:
BACKTEST_STRATEGY_CAPTURE="data/captures/YOUR_CAPTURE" make optimize
```

The optimizer runs sequential per-symbol CMA-ES searches, then validates the
combined candidate on frozen selection capture A and a separate qualifying
validation capture B. Automatic selection uses a chronological pair; fixing A
explicitly permits a different nonoverlapping B that may be earlier.

| Mode | Search specification | Installed symbol profiles |
|---|---|---|
| BUY-only | `optimization.buy.yml` | `SYMBOL.optimization.buy.yml` |
| CTF | `optimization.yml` | `SYMBOL.optimization.yml` |

The specifications define search ranges and budgets. The supplied BUY
specification includes the three learner-fitting L2 penalties and selects by
prediction quality; the CTF specification selects by profit and does not search
those fitting penalties. Worker selection is automatic with `optimizer.jobs: 0`.

**Application is enabled by default.** A candidate can update its symbol's
installed profile and selected learner binding only after structural checks
and the economic gates pass: positive realized net PnL on both A and B, and
improvement over that symbol's incumbent on B. Prediction mode also requires
its prediction improvement checks. Promotion is independent per symbol;
another symbol's losses do not automatically veto a qualifying one.

Run status and summaries are written beneath A at
`backtest_reports/optimizer_runs/RUN_ID/`, including `summary.md` and
`summary.json`. Backtests and optimization share an exclusive local run lane,
so run them one at a time. Historical selection and validation do not establish
future profitability.

## Forks and further work

If you want to continue development or explore a better strategy, fork the
repository. I won't be reviewing or merging pull requests.

For further research, document the strategy rationale, reproducible
capture/replay instructions and results after fees and costs. Report prediction
scores and financial results separately, including losses and validation on
data not used for selection.

## License and disclaimer

The project is licensed under [Apache-2.0](LICENSE), which permits use,
modification and redistribution, including commercial use, subject to its
terms. Preserve the required license and attribution notices. See the
[official license text](https://www.apache.org/licenses/LICENSE-2.0).

Bundled third-party components retain their own licenses. The SDK and CMA-ES
include local modifications; their upstream notices remain in
`vendor/rs-clob-client-v2/LICENSE` and
`crates/cmaes_batch/LICENSE-MIT` / `LICENSE-APACHE`.

**This project is shared for research and education. Nothing in this repository
constitutes financial, investment or trading advice, or a recommendation to buy
or sell any asset.**

Historical accuracy and speed observations describe past development testing.
They do not guarantee future prediction accuracy, reaction time, order execution
or profitability. Trading involves risk of loss. I did not achieve reliable
profitability with this strategy.

The software is provided as-is under the applicable licenses. Their warranty
disclaimers and liability limitations apply subject to applicable law. Users
are responsible for their trading decisions, configuration and compliance with
applicable laws and platform rules.
