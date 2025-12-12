import { LabIcon } from "@jupyterlab/ui-components";

import arrowIPCSvg from "../style/icons/arrow.svg";
import arrowIPCDarkSvg from "../style/icons/arrow_dark.svg";
import avroSvg from "../style/icons/avro.svg";
import orcLightSvg from "../style/icons/orc.svg";
import orcDarkSvg from "../style/icons/orc_light.svg";
import parquetSvgLight from "../style/icons/parquet_dark.svg";
import parquetSvgDark from "../style/icons/parquet_light.svg";

export const getLabIcon = (labIconName: string, iconSvg: string) => {
  return new LabIcon({
    name: `arbalister:${labIconName}`,
    svgstr: iconSvg,
  });
};

export const getIcon = (iconName: string, isLight: boolean) => {
  let icon: LabIcon | undefined;
  switch (iconName) {
    case "parquet":
      icon = getLabIcon(iconName, isLight ? parquetSvgLight : parquetSvgDark);
      break;
    case "arrowipc":
      icon = getLabIcon(iconName, isLight ? arrowIPCSvg : arrowIPCDarkSvg);
      break;
    case "orc":
      icon = getLabIcon(iconName, isLight ? orcLightSvg : orcDarkSvg);
      break;
    case "avro":
      icon = getLabIcon(iconName, isLight ? avroSvg : avroSvg);
      break;
  }
  return icon;
};
