# Doorbell recordings page

Public copy of the phone page for the CG2271 smart doorbell, so recordings can be watched from any network.

- **Sign-in is required.** The page uses a Supabase viewer login, and the data is protected by Supabase row-level security.
- **`config.js` holds only public values:** the project URL and the publishable key. There are no passwords or secret keys here.
- **This repo is only for publishing.** The source and tests live in the team's private project repo under `source/esp32/supabase/viewer/`. Update that copy first, then copy `index.html` here.
