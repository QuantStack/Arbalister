import {LabIcon } from '@jupyterlab/ui-components';
import arrowIPCSvg from '../style/icons/arrow.svg';
import avroSvg from "../style/icons/avro.svg";
import orcSvg from "../style/icons/orc.svg";
import parquetSvg from '../style/icons/parquet_origin.svg';


export const arrowIPC = new LabIcon({
    name: 'arbalister:arrowipc',
    svgstr: arrowIPCSvg
});


export const avroIcon = new LabIcon({
    name: 'arbalister:avro',
    svgstr: avroSvg
});

export const orcIcon = new LabIcon({
    name: 'arbalister:orc',
    svgstr: orcSvg
});

export const parquetIcon = new LabIcon({
    name: 'arbalister:parquet',
    svgstr: parquetSvg
});