# Physiological Full-PA Tuning Model (Operator Guide)

This guide covers the impedance (structured-tree) tuning configuration used for
the PPAS cohort: what each control does, why it was chosen, how svzt-agent
passes and checks it, how to read the results, and what to change when a
patient does not fit.

- Model rationale, evidence and references (authoritative):
  `svZeroDTrees/docs/pulmonary_tuning_model.md`.
- svZeroDTrees config contract for every key: `svZeroDTrees/docs/interface.md`.
- Study that produced the configuration (TST-STAN-5):
  `svZeroDTrees/examples/adaptation/tst5-physiological-tuning-study/`
  (`OPTION1.md`, `FINDINGS.md`).
- Stiffness identifiability study behind the tied stiffness multiplier and
  the xi floor of 2.1 (TST-STAN-3):
  `svZeroDTrees/examples/adaptation/tst3-stiffness-identifiability/FINDINGS.md`.

svzt-agent does not implement any of the model. It validates the
`tuning.impedance` block, merges per-patient overrides, renders the result
into the Slurm job (`ZEROD_TUNING_CONFIG_JSON`), and records the artifacts
svZeroDTrees writes. The boundary is the same as for every other impedance key
(`docs/ARCHITECTURE.md`).

## 1. The model in one paragraph

The 0D seed (learned or calibrated full-PA model of the 3D domain) gets a
structured-tree impedance boundary condition at every outlet cap. Trees are
built at each cap's measured diameter with Olufsen stiffness
Eh/r = k1·exp(k2·r) + k3. Per side, svZeroDTrees tunes the radius exponent
`xi` and the asymmetry `eta_sym`. One shared stiffness multiplier s sets both
the 0D seed vessels' thin-wall Eh/r = s·5e4 dyn/cm² and the trees'
k3 = s·1e5 (§8 item 9); `k1`, `k2`, `lrr` and `d_min` are fixed at literature
values. The outlet pressure Pd comes from a
per-patient policy. The objective is a Gaussian likelihood in mmHg. The
optimizer runs on one shared tree per side, then polishes on the per-cap trees
that are actually exported.

## 2. Configuration reference

All keys live under `tuning.impedance` in `config/defaults.yaml`. Each one can
be overridden per patient in `config/patients.yaml` under
`patients[].tuning.impedance`.

| key | svz default | meaning |
|---|---|---|
| `objective` | `{type: likelihood, pressure_sigma_mmhg: 2.0, split_sigma: 0.02, target_sigma: 1.0}` | Loss = Σ((model − target)/σ)². `target_sigma` sets the optimizer's target stop and the iteration gate (§4). `type: relative` (or omitting the key) restores the historical relative loss. |
| `keep_diastolic_target` | `true` | Keep the diastolic term even when the target is below Pd. |
| `wedge_pressure_policy` | `diastolic_offset` | How Pd is derived (§3). Allowed: `clamp_to_diastolic`, `measured`, `precapillary_fraction`, `diastolic_offset`. |
| `diastolic_offset_mmhg` | `2.0` | Pd = PA diastolic − offset (`diastolic_offset` only). |
| `precapillary_fraction` | `0.332` | Pd = PCWP + f·(mPAP − PCWP) (`precapillary_fraction` only). |
| `proximal_compliance` | `{wall_ehr: 5.0e+4, tied_k3_ref: 1.0e+5}` | Thin-wall C = 3AL/(2·Eh/r) on seed vessels; `null` disables it. Full-PA only; not allowed with `convert_to_cm`. `tied_k3_ref` turns on the tied stiffness multiplier: with a `stiffness_scale` tune-space parameter s, every objective evaluation uses Eh/r = s·`wall_ehr` on **every** seed vessel (C kept from an earlier iteration is overwritten) and k3 = s·`tied_k3_ref` on both trees. Omitted, the rendered block stays `{wall_ehr}`. |
| `tree_max_nodes` | `1000000` | Node budget per structured tree; trees that hit it are flagged as truncated. |
| `leaf_resistance` | unset (evaluated on TST-STAN-5 and not adopted: χ² 50.6 vs 17.2; §8) | `{downstream_fraction: f}`: every tree leaf gets one resistance to the outlet pressure carrying the fraction `f` of the tree's DC resistance (capillary + venous bed). Requires `wedge_pressure_policy: measured` (outlet at PCWP) and `tuning_model: full_pa`. `tuning_diagnostics.json` reports the resulting arterial vs capillary + venous split of PVR (`leaf_resistance`). |
| `polish` | `{maxfev: 100}` | Per-cap Nelder–Mead polish after the shared-tree fit. Needs `tuning_model: full_pa` and `objective_tree_policy`. Optional `initial_simplex_step`. |
| `objective_tree_policy` | `{use_mean: true, reference_diameter: conductance_matched}` | Shared surrogate trees used during optimization. |
| `tuning_model` | `full_pa` | `rri` iterations automatically drop `objective_tree_policy`, `proximal_compliance` and `polish` (`_iteration_impedance_config`). |
| `diameter_scale`, `use_mean` | `1.0`, `false` | Exported trees sit at the measured cap diameters, one per cap. |
| `tune_space` | xi ∈ [2.1, 3.0], eta_sym ∈ [0.3, 1.0] per side; `stiffness_scale` ∈ [0.5, 20] (log, init 1 = the literature point); fixed lrr 10, d_min 0.01, k1 2.6e5, k2 −14 | Free, fixed and tied parameters. svZeroDTrees rejects parameter names it does not recognize. Until 2026-10-16: xi ∈ [2.33, 3.0] and a free `comp.lpa.k3` ∈ [5e4, 4e5] with proximal Eh/r fixed at 5e4. A patient that must keep the old form needs a patient `tune_space` with the free k3 and `proximal_compliance: {wall_ehr: 5.0e+4}`. |

Pydantic validation (`src/svztagent/config/models.py`) rejects:
- `precapillary_fraction` outside [0, 1);
- a negative offset;
- `tree_max_nodes` ≤ 0;
- `leaf_resistance` without `wedge_pressure_policy: measured`, or outside full PA;
- `wall_ehr` ≤ 0;
- an unknown policy;
- `polish` without `objective_tree_policy`;
- `proximal_compliance` or `polish` with `tuning_model: rri`;
- `tied_k3_ref` ≤ 0, `tied_k3_ref` without a free or fixed `stiffness_scale`
  (and the reverse), `stiffness_scale` in `tied`, a `stiffness_scale` lower
  bound (or fixed value) ≤ 0, any `comp.{lpa,rpa}.k3` entry next to
  `stiffness_scale` (k3 is derived from s), or `stiffness_scale` with
  `compliance_model: constant`. svZeroDTrees enforces the same rules
  (`validate_stiffness_scale`), so a dry run fails before submission.

**Override semantics.** A patient override changes only the keys it names.
`objective`, `polish` and `stopping` are patched field by field: for example,
`objective: {target_sigma: 2.0}` keeps the workspace `type: likelihood` and
sigmas. `tune_space`, `objective_tree_policy` and `proximal_compliance` replace
the workspace value as a whole. An explicit `null` clears a workspace default
for that patient. This works for `objective`, `proximal_compliance`, `polish`,
`tree_max_nodes`, `leaf_resistance`, `objective_tree_policy` and the `outlet_mapping*` keys; for
example, `proximal_compliance: null` turns proximal compliance off for one
patient. A patient-level `tuning_model: rri` must also clear
`objective_tree_policy`, `proximal_compliance`, `polish` and the outlet-mapping
keys.

**Scheduler resources.** Tree construction and the 0D solves run in a
single process, so the svz workspace sets `n_procs: 1`, and the tune job
requests exactly `n_procs` CPUs (`--cpus-per-task`; scheduler `cpus` applies
only when `n_procs` is absent). The 3D simulation, prestress, postprocess and
calibration steps are separate Slurm jobs with their own allocations. TST-STAN-5 runs peaked at 3.2–4.5 GB, so the svz default
`mem: "32G"` is ample. Per-cap evaluations take about 65 s for 23 caps, so
the polish (`maxfev` 100) takes about 2 h and the shared stage at most about
1.2 h; the polish scales with the number of caps.

## 3. Outlet pressure policy (per patient)

| patient | policy | Pd | why |
|---|---|---|---|
| no pulmonary regurgitation (default) | `diastolic_offset` | PA diastolic − 2 mmHg | Without backflow, MPA pressure cannot fall below Pd. A Pd just under the diastolic target keeps diastole reachable and needs no wedge pressure. |
| pulmonary regurgitation | `precapillary_fraction` (set in `patients.yaml`) | PCWP + 0.332·(mPAP − PCWP) | A fitted outlet pressure (TST-STAN-5: χ² 33.5 → 17.3 vs Pd = PCWP). It is **not** Dong et al.'s partition, which puts 33.2% of PVR in the arteries; as a constant pressure that would be PCWP + 0.668·(mPAP − PCWP), above PA diastolic for every patient. Backflow lets diastolic pressure fall below Pd. Needs a measured PCWP. See `leaf_resistance` for the pulsatile form of the partition. |

Current cohort assignments: TST-STAN-1 and TST-STAN-5 use `precapillary_fraction`
(both regurgitant). TST-STAN-2, -3 and -9 use the default. The resolved
outlet pressures are 12.98, 12.00, 7.00, 9.99 and 3.00 mmHg for TST-STAN-1, -2,
-3, -5 and -9.

svZeroDTrees fails the tuning job if Pd is not finite, is negative, or is at or
above the mean pressure. It also fails if `precapillary_fraction` is selected
without a measured wedge pressure. These checks run before any tree is built.

## 4. Checks svzt-agent performs

**`svzt config validate` and `svzt doctor`** print "Tuning model warnings"
(`src/svztagent/config/tuning_checks.py`). These are warnings only and never
block a run. Each check reads `config/clinical_targets.yaml` when present:
- `regurgitation: true` but the policy is not `precapillary_fraction`;
- `regurgitation: false` but the policy is `precapillary_fraction`;
- `precapillary_fraction` with no `wedge_pressure` listed;
- `proximal_compliance.wall_ehr` more than 2× outside the deformable 3D wall's
  E·h/r over r ∈ [0.2, 1.0] cm. The 3D model replaces the same vessels, so a
  mismatched wall means the 3D-coupled model will not reproduce the tuned 0D
  proximal compliance (§8, decision 1);
- the tied stiffness multiplier with a fixed numeric `elasticity_modulus` on a
  deformable 3D wall: s changes the proximal Eh/r every iteration, so only
  `match_proximal_compliance` keeps the 3D wall matched.

**Job preflight (exit code 8).** Before any work, the Slurm job compares the
rendered impedance keys with `svzerodtrees.tuning.SUPPORTED_IMPEDANCE_KEYS`.
If an older svZeroDTrees lacks that symbol, the job instead checks for the new
features directly. When the cluster's svZeroDTrees is too old for the
configuration, the job exits with code 8 and names the unsupported keys; it
does not silently ignore them. Fix: `svzt update` (or install the current
svZeroDTrees on the cluster), then resubmit. The preflight checks top-level
keys only: an svZeroDTrees older than the tied stiffness multiplier fails at
the start of tuning instead ("Unknown keys in ... proximal_compliance:
['tied_k3_ref']"), so update the cluster install before submitting
any patient.

**Iteration gate in sigma mode.** With `objective.type: likelihood` and a
non-null `target_sigma`, the 3D iteration gate and the 0D pre-mapping gate both
accept a metric when |model − target| ≤ `target_sigma` × σ. The σ values are
2 mmHg for systolic, diastolic and mean pressure and 0.02 for the RPA split.
Otherwise the historical 10% relative gate applies, which would demand
0.3 mmHg on a 3 mmHg diastolic target. Set `target_sigma: null` to keep the
likelihood loss with the relative gate.

## 5. Artifacts

svZeroDTrees writes these into the iteration's `results/` directory. The job
records the paths of `optimized_params.csv`, `seed_with_proximal_compliance.json`
and `tuning_diagnostics.json` in `iteration_decision.json` under
`tuning_artifacts`.

| file | content |
|---|---|
| `optimized_params.csv` | Final parameters, after the polish when it ran. |
| `optimized_params_shared.csv`, `pa_config_tuning_snapshot_shared.json` | Shared-tree optimum before the polish (only when `polish` is set). Written next to the other results but not listed in `tuning_artifacts`. |
| `seed_with_proximal_compliance.json` | The seed actually tuned, with proximal C filled in. The original seed is never modified. With the tied stiffness multiplier it holds C at the selected s, and `tuning_diagnostics.json` `proximal_compliance` adds `stiffness_scale`, `wall_ehr_ref`, `tied_k3_ref` and `tree_k3`; its `wall_ehr` and `matched_uniform_wall_eh` are at the selected s, so `match_proximal_compliance` follows it. `optimized_params.csv` gains `stiffness_scale` and `proximal_wall_ehr` columns; `stiffness_scale` seeds the next iteration. |
| `tuning_diagnostics.json` | Fit and model-plausibility summary (§6). |

## 6. Reading `tuning_diagnostics.json`

| field | look for |
|---|---|
| `published_fit` | Simulated sys/dia/mean, split, `errors_mmhg`, and `chi2` of the exported model (per-cap trees, the one used in 3D). This is the number that matters. Each term above 1 means more than 1σ off. If scoring fails, it holds an `error` and the rest of the file is still written. |
| `inflow_consistency` | Mean flow of the exported model vs `inflow.csv`. The optimizer runs at the `inflow.csv` mean; the exported model keeps the seed's inflow. `consistent: false` (more than 1% apart) means the published fit and the optimizer fit were computed at different flows. |
| `optimizer_fit` | The optimizer's own estimate. A gap of more than ~1 mmHg from `published_fit` means the surrogate is biased (normally removed by `polish`). |
| `polish` | `used` is `per_cap` or `shared` (fallback after a polish failure), `maxfev`, and `shared_fit` / `polished_fit`, whose `loss` equals χ² under the likelihood objective. |
| `parameters_at_bounds` | Parameters within 1% of a bound: the data want values outside the literature ranges (§7). |
| `model_total_compliance_ml_per_mmhg`, `svpp_bracket`, `compliance_in_svpp_bracket` | Total compliance (trees + proximal) vs [regurgitant volume/PP, forward SV/PP]. Outside the bracket means compliance is implausible even if pressures fit. |
| `trees.n_truncated` | Trees that hit `tree_max_nodes`. These have truncated resistance and compliance, so raise the budget. |
| `outlet_pressure` | Pd, the policy, and its inputs. Confirm this is the intended policy. |
| `svzerodtrees_version`, `peak_rss_gb` | Provenance and memory sizing. |

Published-model scoring retries the solver at a looser absolute tolerance
(1e-7 → 1e-5) when Newton iterations fail, and records the tolerance used.

## 7. Why each choice (summary)

Full evidence is in `svZeroDTrees/docs/pulmonary_tuning_model.md` §2.

- **Likelihood objective (σ = 2 mmHg, 0.02).** The relative loss weighted one
  diastolic mmHg about 100× one systolic mmHg. With it, the optimizer traded
  more than 20 mmHg of systolic and mean error for diastolic. σ is the
  catheter and MRI measurement accuracy, so χ² is interpretable.
- **Outlet policies.** These are explained in §3. Pd = PCWP in a regurgitant
  patient over-constrains the tree; TST-STAN-5's χ² fell from 33.5 to 17.3
  with Pd = PCWP + 0.332·(mPAP − PCWP). That fraction is fitted, not taken
  from Dong et al. (§8, item 8).
- **Proximal compliance, Eh/r = 5e4.** Learned seeds are rigid, so all
  compliance had to sit in the trees behind a high proximal impedance. 5e4 is
  the Krenz & Dawson pulmonary-artery value. With it, TST-STAN-5 fit with
  literature tree stiffness. Without it, tree stiffness had to go to k3 = 0.
- **Literature stiffness (k1 2.6e5, k2 −14), one tied multiplier for k3 and
  the proximal wall.** k2 is not identifiable from the four targets (Paun
  2020). The proximal Eh/r and k3 are identifiable only together (§8 item 9),
  so one s ∈ [0.5, 20] scales both from the literature point (5e4, 1e5).
- **xi ∈ [2.1, 3], eta_sym ∈ [0.3, 1], per side.** The xi floor is the lowest
  measured pulmonary value (Miles et al. 2025), not Uylings' theoretical 2.33;
  eta spans measured asymmetry. Both are free per side so the split can be
  fit.
- **lrr 10, d_min 0.01 cm.** Mid-literature length ratio (7–13) and capillary
  cutoff.
- **Measured cap diameters, 1M nodes.** These are the published geometry. The
  1M budget keeps the largest caps from truncating.
- **Shared fit, then per-cap polish.** Shared trees are about 9× cheaper per
  evaluation but leave a ~2.6 mmHg gap to the published model. The polish
  closes it to 0.05 mmHg.

## 8. Decisions and known limitations before running the cohort

1. **3D wall softened to the 0D proximal compliance (decided 2026-10-06).**
   The deformable 3D wall uses E = 1.375e5 dyn/cm² at h = 0.2 cm (was
   2.5e6). A uniform wall carries the same total compliance as the 0D
   proximal compliance (C = 3AL/(2·`wall_ehr`) per vessel) when
   E·h = `wall_ehr` · r_eff, with r_eff = Σ(A·L·r)/Σ(A·L) the volume-weighted
   radius of the seed vessels. For TST-STAN-5, r_eff = 0.55 cm, so
   E·h = 2.75e4 dyn/cm. The uniform wall then gives E·h/r from 3.5e4 (MPA) to
   1.5e5 (the smallest branches): it puts more of the compliance in the
   largest vessels than the 0D model does, but the total matches.
   - Each patient's matched value is in `tuning_diagnostics.json`
     (`proximal_compliance.matched_uniform_wall_eh`). The job records the
     ratio of the 3D wall to it in `iteration_driver_log.json`
     (`threed_wall_vs_proximal_compliance`), with a warning outside 0.5–2×.
   - `tuning.threed.elasticity_modulus: "match_proximal_compliance"` (the
     workspace default since 2026-10-09) sets E = `matched_uniform_wall_eh` / h
     per patient and iteration, from that iteration's `tuning_diagnostics.json`,
     before any deformable 3D stage (preop, prestress; postop and adaptation
     use the selected preop iteration's value). The driver log records it as
     `threed_elasticity_modulus`; without `proximal_compliance` the iteration
     goes to needs-review instead of running 3D. r_eff varies with PA size
     (TST-STAN-1 0.47 cm to TST-STAN-9 0.98 cm), so one shared E left the 3D
     wall up to 1.8× more compliant than the 0D model.
   - Only E changed, so the precomputed prestress files (TST-STAN-1, -5)
     remain usable: with uniform wall properties, the prestress that balances
     the mean traction is essentially independent of E. Tissue support carries
     only a few percent of the load at this stiffness.
   - Caveat: Eh/r = 5e4 corresponds to Krenz & Dawson's 2%/mmHg distensibility
     (Eh/r = 3/(4α)). Over TST-STAN-5's 31 mmHg pulse pressure, the linear
     wall changes the proximal volume by about 120% (0.63 mL/mmHg × 31 mmHg
     vs a 15.6 mL proximal volume). The 0D model already assumes this, but in
     3D the wall motion is far beyond the coupled-momentum method's
     small-strain assumption and beyond MRI-observed pulsatility (about
     20–40% area change, i.e. Eh/r ≈ 1.5–3e5). Patient-measured proximal
     stiffness is the first future change (§9).
2. **k3 stays free (decided 2026-10-06; superseded 2026-10-16 by item 9,
   which derives k3 from the tied multiplier).** In TST-STAN-5, fixing k3 at 1e5
   cost about 1 χ² (18.3 vs 17.2) and kept total compliance inside the SV/PP
   bracket; free k3 reached 5e4 and left it. Keeping k3 free lets patients with
   stiffer vessels (e.g. higher pressures) move it. Check
   `compliance_in_svpp_bracket` and `parameters_at_bounds` per patient; fixing
   k3 at 1e5 (move `comp.lpa.k3` to `fixed`) is the fallback.
3. **The 3D convergence gate is ignored for now; iterations are hand-picked
   (decided 2026-10-06).** Some patients cannot pass the per-metric gate:
   TST-STAN-5's best 0D fit misses diastolic by −7.7 mmHg (3.8σ). Every
   iteration then reports `not_close`, and the driver runs to
   `max_iterations` (default 5). Pick the preop iteration to carry forward
   with `svzt preop select --run-id <id> --iteration <n> --reason <text>`,
   which needs a completed preop 3D run, not a `converged` decision. The gate
   (or a χ²-based replacement) will be revisited later.
4. **Start fresh runs.** Each submission re-resolves the impedance config from
   the current YAML and seeds the optimizer from the previous iteration's
   `optimized_params.csv`. A run started under the old configuration (e.g.
   k3 = 0, or xi above 3) has values outside the new bounds, and its next
   iteration fails into `needs_review`. Initialize a new run per patient.
5. **Adaptation: use M2.** `svzt run adapt` passes the selected iteration's
   `results/svzerod_3d_coupling_tuned.json` and `outlet_cap_mapping.json`
   (required for full-PA iterations) to svZeroDTrees. M2 rebuilds every tuned
   per-cap tree from its metadata (tuned parameters, measured diameter,
   `max_nodes`), pairs caps and BCs through the saved outlet mapping, uses the
   tuned Pd, and applies the LPA/RPA territory update to every tree on that
   side. M1 and M3 adapt one tree per side in a reduced-order PA model and are
   rejected for full-PA runs at plan time. Open decisions (per-cap vs
   territory stimulus for M2; how M1/M3 should take per-cap trees) are in
   `svZeroDTrees/docs/pulmonary_tuning_model.md` §7. Adaptation stops with an
   error if the postop 3D coupler couples a BC to a different cap than the
   outlet mapping. svZeroDTrees `generate_threed_coupler` currently assigns
   coupler surfaces by position, so check this on the first patient.
6. **Inflow provenance.** TST-STAN-1's model inflow is TST-STAN-5's waveform
   rescaled. Confirm each patient's inflow before tuning.
7. **Flow source.** In TST-STAN-5, cath flow (2.79 L/min) differs from MRI net
   flow (1.55 L/min). Tree resistance scales with the flow used.
8. **Outlet pressure: the 0.332 rule is fitted, not Dong et al.; leaf
   resistance evaluated and not adopted (2026-10-06).** Dong et al. 2021
   (after Raj & Chen 1986) put 33.2% of PVR in the arteries and 66.8% in the
   capillaries and veins. They used it only to size steady distal trees; their
   pulsatile model used RCR outlets with distal pressure = LA. As a constant
   outlet pressure the partition (PCWP + 0.668·TPG) sits 6–14 mmHg above PA
   diastolic for every patient. Its pulsatile form, outlet at PCWP plus a
   capillary + venous resistance at the tree leaves (`leaf_resistance`), was
   tested on TST-STAN-5 with the same budget (published model):

   | case | Pd | sys / dia / mean (34 / 3 / 16) | χ² |
   |---|---|---|---|
   | `precapillary_fraction` 0.332 (current) | 9.99 | 36.8 / −4.7 / 14.6 | 17.2 |
   | leaf share 0.668 | 7.0 | 46.0 / −4.3 / 17.9 | 50.6 |
   | leaf share 0.90 | 7.0 | 54.0 / 6.6 / 28.6 | ~145 |

   The leaf resistance sits behind the tree compliance and slows its
   drainage, so pulse pressure rises with the regurgitant flow swing, while
   the backflow still drains diastolic. The fit is bound-limited (ξ = 3, k3 at
   5e4) in every case. The current policies stay; cite the 0.332 as an
   empirically fitted outlet pressure. `leaf_resistance` remains available
   (off) for testing on patients without regurgitation. Details:
   `svZeroDTrees/docs/pulmonary_tuning_model.md` §2.3.

9. **Tied stiffness multiplier and xi ≥ 2.1, cohort default (decided
   2026-10-16).** Under the full-PA objective the proximal wall Eh/r and the
   tree k3 act on systolic and diastolic pressure with nearly the same
   signature (corr −0.99); above k3 ≈ 8e5 only their combination, total
   compliance ≈ 0.45 mL/mmHg, is identified. Freeing both adds an
   unresolvable direction; fixing a cohort k3 does not fit both TST-STAN-3
   (wants k3 ≈ 7.5e5) and TST-STAN-5 (wanted the lower bound). So
   the cohort tunes one s: Eh/r = s·5e4, k3 = s·1e5, s ∈ [0.5, 20] in log
   space (below 1 so softer patients can fit too). The remaining mean-pressure and split
   shortfall came from the rpa xi floor 2.33 (Uylings' theoretical value);
   measured pulmonary xi go down to 2.1 (Miles et al. 2025). With xi ≥ 2.1 and
   lrr fixed at 10 the study fit is χ² 0.29 (67.3/9.6/31.2 mmHg, split 0.445;
   targets 67/9/32, 0.446) at s 6.56, rpa xi 2.17 inside the bound. A free lrr
   was rejected: it is not identifiable (lrr 14 and 20 both fit). The xi
   floor does not bind for TST-STAN-5 (xi at the upper bound 3). Neither
   change was fitted on TST-STAN-1, -2, -5 or -9 before adoption; check their
   `parameters_at_bounds` and `compliance_in_svpp_bracket`. Study caveats:
   shared conductance-matched trees, the iter-02 seed, local periodic-cycle
   solves.
   - In-flight runs keep working: their next iteration re-resolves this
     config, earlier xi/eta lie inside the new bounds, the old CSV has no
     `stiffness_scale` (so s starts at 1), and its k3 column is ignored.
     Proximal C of a calibrated seed is recomputed from geometry at the
     tuned s rather than kept.

## 9. If a patient does not fit

Read `tuning_diagnostics.json` first. Then use the symptom → remedy table in
`svZeroDTrees/docs/pulmonary_tuning_model.md` §6. The most common config-level
remedies are:

| symptom | first change |
|---|---|
| Diastolic far too low (regurgitant) | Known limitation. Check `outlet_pressure`; consider measured proximal stiffness (lower `wall_ehr` only with MRI distensibility data). |
| Pulse too small / diastolic too high (no PR) | Check `stiffness_scale` in `parameters_at_bounds`; raise its upper bound if it sits at 20. Check `diastolic_offset_mmhg`. |
| Mean off with xi or eta at a bound | Check the flow source and the outlet policy; consider lrr within 7–13. |
| Many truncated trees | Raise `tree_max_nodes` and memory (~5 GB per 1M nodes). |
| Published far from optimizer fit | Keep `polish`; raise `polish.maxfev`. |
| Exit code 8 | Update svZeroDTrees on the cluster. |
| Pd infeasible error | Switch to `diastolic_offset`, or add a measured PCWP. |

**Future model changes, in priority order** (details in the svZeroDTrees
doc):
1. Patient-measured proximal stiffness from MRI pulsatility.
2. Time-resolved targets: the cath pressure trace and branch flow waveforms.
3. An RV plus regurgitant-valve inlet for patients with pulmonary
   regurgitation.
4. Seed recalibration against 3D with a 3D wall consistent with the 0D.
5. A compliance-matched shared surrogate.
6. Bayesian calibration.
7. A strain-stiffening wall law.
8. Age-scaled morphometry and multi-patient validation.
