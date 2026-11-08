# Doorbell page

Public copy of the phone page for the CG2271 smart doorbell, so it works from any network.

- **Recordings:** list and play the doorbell's recorded clips.
- **Front door** (`live.js`): live doorbell status, Answer / End call, live camera, call audio and hold-to-talk, over private Supabase Realtime channels.

**Sign-in is required.** The page uses a Supabase viewer login. The data and the live channels are protected by Supabase row-level security and Realtime authorization (`realtime.sql`).

`config.js` holds only public values: the project URL and the publishable key. There are no passwords or secret keys here.

**This repo is only for publishing.** The source and tests live in the team's private project repo under `source/esp32/supabase/viewer/`. Update `index.html` and `live.js` there first, then copy both here.
