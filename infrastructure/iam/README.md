# IAM — Glue service role

Glue Crawlers and ETL jobs don't run as your IAM user. They run as a **service
role** that Glue assumes on your behalf. That role needs two things:

1. **A trust policy** — who is allowed to assume the role. For Glue this is the
   `glue.amazonaws.com` service principal. That's what `glue_service_role.json`
   contains.
2. **Permission policies** — what the role can do once assumed. For this
   project: read/write the data lake bucket and read/write the Glue Catalog.

## Role in use

```
Role name : AWSGlueServiceRole-DataLakeTutorial
ARN       : arn:aws:iam::055622654653:role/AWSGlueServiceRole-DataLakeTutorial
```

> AWS console convention: a Glue role name **must** start with `AWSGlueServiceRole`
> for the console's role picker to surface it. Hence the prefix.

## Recreate it from scratch

```bash
# 1. Create the role with the trust policy in this folder
aws iam create-role \
  --role-name AWSGlueServiceRole-DataLakeTutorial \
  --assume-role-policy-document file://glue_service_role.json

# 2. Attach the AWS-managed Glue policy (Catalog + CloudWatch logs access)
aws iam attach-role-policy \
  --role-name AWSGlueServiceRole-DataLakeTutorial \
  --policy-arn arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole

# 3. Grant access to the data lake bucket only (least privilege — not S3FullAccess)
aws iam put-role-policy \
  --role-name AWSGlueServiceRole-DataLakeTutorial \
  --policy-name DataLakeBucketAccess \
  --policy-document '{
    "Version": "2012-10-17",
    "Statement": [
      {
        "Effect": "Allow",
        "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"],
        "Resource": "arn:aws:s3:::marco-data-darya/*"
      },
      {
        "Effect": "Allow",
        "Action": ["s3:ListBucket"],
        "Resource": "arn:aws:s3:::marco-data-darya"
      }
    ]
  }'
```

## Why not just give it `AmazonS3FullAccess`?

Because the crawler only needs the one bucket. Scoping the policy to
`marco-data-darya` means a misconfigured job can't read or clobber unrelated
buckets in the account. Least privilege is cheap insurance.
