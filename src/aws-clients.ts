import { ECSClient } from "@aws-sdk/client-ecs";
import { ElasticLoadBalancingV2Client } from "@aws-sdk/client-elastic-load-balancing-v2";
import { RDSClient } from "@aws-sdk/client-rds";
import { SecretsManagerClient } from "@aws-sdk/client-secrets-manager";
import { SSMClient } from "@aws-sdk/client-ssm";

/**
 * Holds the AWS SDK clients, created once. Region and credentials come from the
 * ambient AWS environment (AWS_REGION plus the role the GitLab job assumes).
 */
export class AwsClients {
  readonly ssm = new SSMClient({});
  readonly ecs = new ECSClient({});
  readonly elbv2 = new ElasticLoadBalancingV2Client({});
  readonly rds = new RDSClient({});
  readonly secrets = new SecretsManagerClient({});
}
