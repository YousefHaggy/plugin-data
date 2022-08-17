/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * Licensed under the BSD 3-Clause license.
 * For full license text, see LICENSE.txt file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
import * as os from 'os';
import * as fs from 'fs';
import { ReadStream } from 'fs';
import parse = require('csv-parse');
import { flags, FlagsConfig } from '@salesforce/command';
import { Connection, Messages } from '@salesforce/core';
import { JobInfoV2 } from 'jsforce/lib/api/bulk';
import { DataCommand } from '../../../../dataCommand';

Messages.importMessagesDirectory(__dirname);
const messages = Messages.loadMessages('@salesforce/plugin-data', 'bulkv2.upsert');

export default class Upsert extends DataCommand {
  public static readonly description = messages.getMessage('description');
  public static readonly examples = messages.getMessage('examples').split(os.EOL);
  public static readonly requiresUsername = true;
  public static readonly flagsConfig: FlagsConfig = {
    externalid: flags.string({
      char: 'i',
      description: messages.getMessage('flags.externalid'),
      required: true,
    }),
    csvfile: flags.filepath({
      char: 'f',
      description: messages.getMessage('flags.csvfile'),
      required: true,
    }),
    sobjecttype: flags.string({
      char: 's',
      description: messages.getMessage('flags.sobjecttype'),
      required: true,
    }),
    assignmentruleid: flags.string({
      description: messages.getMessage('flags.assignmentruleid'),
    }),
  };

  public async run(): Promise<Partial<JobInfoV2>> {
    const { sobjecttype, externalid, csvfile, assignmentruleid } = this.flags;
    const conn: Connection = this.ensureOrg().getConnection();
    this.ux.startSpinner('Bulk Upsert');

    await this.throwIfPathDoesntExist(this.flags.csvfile as string);

    const csvStream: ReadStream = fs.createReadStream(csvfile as string, { encoding: 'utf-8' });

    const records: Array<Record<string, string>> = [];
    const parser = parse({
      columns: true,
      // library option is snakecase
      // eslint-disable-next-line camelcase
      skip_empty_lines: true,
      bom: true,
    });

    await new Promise<void>((resolve) => {
      csvStream
        .pipe(parser)
        .on('data', (row) => {
          records.push(row);
        })
        .on('end', () => {
          resolve();
        });
    });

    const job = conn.bulk2.createJob({
      object: sobjecttype as string,
      operation: 'upsert',
      externalIdFieldName: externalid as string,
      assignmentRuleId: assignmentruleid as string,
    });

    await job.open();
    await job.uploadData(records);
    await job.close();
    this.ux.log(messages.getMessage('CheckStatusCommand', [job.id, job.id]));
    this.ux.stopSpinner();

    return job.jobInfo;
  }
}
