import { register } from "node:module";

globalThis.Cloudflare = {
  compatibilityFlags: { global_fetch_strictly_public: true },
};

register("./test-loader.mjs", import.meta.url);
