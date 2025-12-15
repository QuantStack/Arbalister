import { LabIcon } from "@jupyterlab/ui-components";

import arrowIPCSvg from "../style/icons/arrow.svg";
import arrowIPCDarkSvg from "../style/icons/arrow_dark.svg";
import avroSvg from "../style/icons/avro.svg";
import orcLightSvg from "../style/icons/orc.svg";
import orcDarkSvg from "../style/icons/orc_dark.svg";
import parquetSvgLight from "../style/icons/parquet.svg";
import parquetSvgDark from "../style/icons/parquet_dark.svg";

export const getLabIcon = (labIconName: string, iconSvg: string) => {
  return new LabIcon({
    name: `arbalister:${labIconName}`,
    svgstr: iconSvg,
  });
};

export const getParquetIcon = (isLight: boolean) => {
  return getLabIcon("parquet", isLight ? parquetSvgLight : parquetSvgDark);
};

export const getArrowIPCIcon = (isLight: boolean) => {
  return getLabIcon("arrowipc", isLight ? arrowIPCSvg : arrowIPCDarkSvg);
};
export const getORCIcon = (isLight: boolean) => {
  return getLabIcon("orc", isLight ? orcLightSvg : orcDarkSvg);
};
export const getAvroIcon = (isLight: boolean) => {
  return getLabIcon("avro", isLight ? avroSvg : avroSvg);
};
