/**
 * Configuration for Playwright using default from @jupyterlab/galata
 */
const baseConfig = require("@jupyterlab/galata/lib/playwright-config");

module.exports = {
  ...baseConfig,
  webServer: {
    command: "jlpm start",
    url: "http://localhost:53729/lab",
    timeout: 120 * 1000,
    reuseExistingServer: !process.env.CI,
  },
  use: {
    ...baseConfig.use,
    baseURL: "http://127.0.0.1:53729",
  },
};
