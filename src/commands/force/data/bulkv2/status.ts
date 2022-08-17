/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * Licensed under the BSD 3-Clause license.
 * For full license text, see LICENSE.txt file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
import * as os from 'os';
import { flags, FlagsConfig } from '@salesforce/command';
import { Connection, Messages } from '@salesforce/core';
import { IngestJobV2Results, JobInfoV2 } from 'jsforce/lib/api/bulk';

import { Schema } from 'jsforce';
import { DataCommand } from '../../../../dataCommand';

Messages.importMessagesDirectory(__dirname);
const messages = Messages.loadMessages('@salesforce/plugin-data', 'bulkv2.status');

export default class Status extends DataCommand {
  public static readonly description = messages.getMessage('description');
  public static readonly examples = messages.getMessage('examples').split(os.EOL);
  public static readonly requiresUsername = true;
  public static readonly flagsConfig: FlagsConfig = {
    jobid: flags.string({
      char: 'i',
      description: messages.getMessage('flags.jobid'),
      required: true,
    }),
    showrecords: flags.boolean({
      char: 'r',
      description: messages.getMessage('flags.showrecords'),
      default: false,
    }),
  };

  public async run(): Promise<IngestJobV2Results<Schema> | Partial<JobInfoV2>> {
    const { jobid, showrecords } = this.flags;
    this.ux.startSpinner('Getting Status');
    const conn: Connection = this.ensureOrg().getConnection();
    // TODO: error handling
    const job = conn.bulk2.job({ id: jobid as string });
    await job.check();
    this.ux.stopSpinner();
    this.ux.styledHeader('BulkV2 Job Status');
    this.ux.styledObject(job.jobInfo);

    if (showrecords) {
      const results = await job.getAllResults();
      this.ux.styledHeader('results');
      this.ux.styledObject(results);
      return { ...job.jobInfo, ...results };
    }
    return job.jobInfo;
  }
}
