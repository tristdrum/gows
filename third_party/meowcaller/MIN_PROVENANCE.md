# Min calling dependency

Vendored from `purpshell/meowcaller` commit
`27a3c6b18657614c9ec2ed16dfc497eff11de6ec` (MIT; license preserved).
Min keeps its existing WhatsMeow replacement at `868907e7d4878c6cb0d8b54f04bdee2e2664b547`.

Two focused Min patches expose authoritative group state for rejection and retire
registered calls when sending an outbound offer fails. Neither adds permission,
redial, logging or diagnostics. Focused regression tests accompany both patches.
The complete upstream source/tests are retained for reproducible maintenance;
upstream AGENTS guidance applies when editing this directory.
