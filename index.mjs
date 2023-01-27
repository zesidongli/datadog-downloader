#!/usr/bin/env node

import { v2 } from "@datadog/datadog-api-client";
import chalk from "chalk";
import * as dotenv from "dotenv";
import * as fs from "fs";
import yargs from "yargs";
const argv = yargs(process.argv).argv;

dotenv.config();

const configuration = v2.createConfiguration();
const apiInstance = new v2.LogsApi(configuration);

async function getLogs(apiInstance, params) {
    const data = [];

    let nextPage = null;
    let n = 0;
    do {
        console.warn(`Requesting page ${++n} ${nextPage ? `with cursor ${nextPage} ` : ``}`);
        const query = nextPage ? { ...params, pageCursor: nextPage } : params;
        const result = await apiInstance.listLogsGet(query);
        data.push(...result.data);
        nextPage = result?.meta?.page?.after;
        console.warn(`${result.data.length} results (${data.length} total)`);
    } while (nextPage);

    return data;
}

function oneYearAgo() {
    return new Date(new Date().setFullYear(new Date().getFullYear() - 1));
}

const initialParams = {
    filterQuery: argv.query,
    filterIndex: argv.index ?? "main",
    filterFrom: argv.from ? new Date(argv.from) : oneYearAgo(),
    filterTo: argv.top ? new Date(argv.to) : new Date(),
    pageLimit: argv.pageSize ? Math.min(argv.pageSize, 5000) : 1000,
};

if (!initialParams.filterQuery) {
    console.warn(chalk.red("Error: No query supplied, use --query"));
    process.exit();
}

console.warn(chalk.cyan("Downloading logs:\n" + JSON.stringify(initialParams, null, 2) + "\n"));

(async function () {
    let data;
    try {
        data = await getLogs(apiInstance, initialParams);
    } catch (e) {
        console.warn(chalk.red(e.message));
        process.exit(1);
    }

    const outputFile = argv.output ?? "results.json";
    console.warn(chalk.cyan(`\nWriting ${data.length} logs to ${outputFile}`));
    // console.warn(JSON.stringify(data, null, 2));
    for (let datum of data) {
        // console.warn(chalk.cyan(`${data[0].id}`));
        // console.warn(chalk.cyan(`${data[0].type}`));
        // console.warn(chalk.cyan(`${data[0].attributes}`));
        // console.warn(chalk.cyan(`${data[0].attributes.attributes.op}`));
        // console.warn(chalk.cyan(`${data[0].attributes.attributes.job_message}`));
        //fs.writeFileSync(outputFile, JSON.stringify(data, null, 2));

        const attribute = datum.attributes
        // const attributes = attribute?.attributes
        // console.warn("INSERT INTO etl._temp_investigate_FWM_dd_logs ( log_id, log_level, logged_at, message, op_token, job_name, job_description)" +
        // ` VALUES ( '${datum.id}', '${attributes?.level}', '${attributes?.timestamp}', '${attribute?.message}', '${attributes?.op}', '${attributes?.job}', '${attributes?.job_message?.description}');`)

        const attributes = attribute.attributes
        console.log("INSERT INTO etl._temp_investigate_FWM_dd_logs ( log_id, log_level, logged_at, message, op_token, job_name, job_description)" +
        ` VALUES ( '${datum.id}', '${attributes.level}', '${attributes.timestamp}', '${attribute.message}', '${attributes.op}', '${attributes.job}', '${attributes.job_message.description}');`)
        //console.warn(JSON.stringify(datum, null, 2));
        
    }
    
    console.warn(chalk.green("Done!"));
})();
//node index.mjs --query 'env:"production" @queue:"outgoing-notifications-sender" service:"backend-worker"' --from 2023-01-23T00:00:00.000Z --to 2023-01-23T12:00:00.000Z > output.sql
