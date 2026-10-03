# Min calling dependency

Vendored from `purpshell/meowcaller` commit
`27a3c6b18657614c9ec2ed16dfc497eff11de6ec` (MIT; license preserved).
Min keeps its existing WhatsMeow replacement at `868907e7d4878c6cb0d8b54f04bdee2e2664b547`.

Focused Min patches expose authoritative group state for rejection and retire
registered calls when sending an outbound offer fails. Uncertain offers receive
one bounded terminate attempt before their metadata is retired, and return the
ended handle for attempt-ID recovery. No offer is retried, and no diagnostics or
permission expansion is enabled. Focused regression tests accompany these patches.
The complete upstream source/tests are retained for reproducible maintenance;
upstream AGENTS guidance applies when editing this directory.

The deferred-accept patch retains the first authenticated setup mute signal and
requires both that signal and application Answer before sending one acceptance.
Observed destination/creator metadata is preserved, later mute and duplicate
Answer never resend, and the existing signaling sender is reused. Ordering and
concurrency regressions pass; real WhatsApp media remains a separate acceptance gate.
