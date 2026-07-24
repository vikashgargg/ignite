import { createVegaLoader } from "../../.vitepress/theme/utils/vega";

export default createVegaLoader(["./*.vega.json"], __dirname, {
  "resource-utilization.vega.json": {
    spark: [{ name: "engine", value: "Spark" }],
    zelox: [{ name: "engine", value: "Zelox" }],
  },
});
