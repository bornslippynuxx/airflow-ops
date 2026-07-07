import { GetParameterCommand, SSMClient } from "@aws-sdk/client-ssm";

/** Shape of the `/airflow/persist` parameter (field name = JSON key). */
export interface PersistConfig {
  dbInstanceIdentifier: string;
  dbSecretArn: string;
  httpsListenerArn: string;
}

/** Shape of each `/airflow/<stack>` parameter. */
export interface RuntimeConfig {
  clusterArn: string;
  taskDefinitionArn: string;
  privateSubnets: string[];
  serviceSecurityGroups: string[];
}

/**
 * Reads a stack's config from SSM Parameter Store. Each stack publishes its
 * config as one JSON parameter; this reads it and parses it into a typed object.
 * CONFIRM the parameter paths against your stacks.
 */
export class ConfigStore {
  static readonly PERSIST_PARAM = "/airflow/persist";

  static runtimeParam(stack: string): string {
    return `/airflow/${stack}`;
  }

  constructor(private readonly ssm: SSMClient) {}

  async read<T>(paramName: string): Promise<T> {
    let value: string | undefined;
    try {
      const res = await this.ssm.send(new GetParameterCommand({ Name: paramName, WithDecryption: true }));
      value = res.Parameter?.Value;
    } catch (e) {
      throw new Error(`Could not read SSM parameter "${paramName}": ${(e as Error).message}`);
    }
    if (!value) throw new Error(`SSM parameter "${paramName}" is empty or missing.`);
    try {
      return JSON.parse(value) as T;
    } catch {
      throw new Error(`SSM parameter "${paramName}" is not valid JSON.`);
    }
  }
}
