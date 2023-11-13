const { app } = require("@azure/functions");
const { BigQuery } = require("@google-cloud/bigquery");
const { SecretClient } = require("@azure/keyvault-secrets");
const { DefaultAzureCredential } = require("@azure/identity");
const contentful = require('contentful-management');

const {
    AzureVaultName,
    ContentfulAccessTokenName,
    BigQueryTokenName,
    BigQueryDatasetId,
    BigQueryTableId
} = process.env;

const secretClient = new SecretClient(`https://${AzureVaultName}.vault.azure.net`, new DefaultAzureCredential());

app.http("contentfulEventsHandler", {
    methods: ["POST"],
    authLevel: "anonymous",
    handler: async (request, context) => {
        try {
            const [contentfulAccessToken, bigQueryToken] = await Promise.all([
                secretClient.getSecret(ContentfulAccessTokenName),
                secretClient.getSecret(BigQueryTokenName)
            ]);

            const contentfulAccessTokenValue = contentfulAccessToken.value;
            const bigQueryTokenValue = JSON.parse(bigQueryToken.value);
            bigQueryTokenValue.private_key = bigQueryTokenValue.private_key.replace(/\\n/g, "\n");

            const client = contentful.createClient({ accessToken: contentfulAccessTokenValue });
            const bigquery = new BigQuery({ credentials: bigQueryTokenValue });

            const requestBody = await request.json();
            context.log("Received Contentful Webhook data:", requestBody);

            if (!requestBody.sys.space.sys.id || !requestBody.sys.createdAt || !requestBody.sys.environment.sys.id) {
                context.error("Missing or undefined properties in the Contentful Webhook payload");
                return {
                    status: 400,
                    body: "Missing or undefined properties in the Contentful Webhook payload",
                };
            }

            const contentfulSpaceId = requestBody.sys.space.sys.id;
            const date = new Date(requestBody.sys.createdAt).toISOString().slice(0, -1);
            const actions = request.headers.get('x-contentful-topic').split(".").pop();
            const userId = request.query.get('user');
            const environment = requestBody.sys.environment.sys.id;

            const space = await client.getSpace(contentfulSpaceId);

            try {
                const bigQueryRow = {
                    contentfulSpace: space.name,
                    date: date,
                    actions: actions,
                    user: userId,
                    environment: environment,
                }

                await bigquery
                    .dataset(BigQueryDatasetId)
                    .table(BigQueryTableId)
                    .insert([bigQueryRow]);

                context.log("Data saved to BigQuery.", bigQueryRow);

                return {
                    status: 200,
                    body: "Webhook received and processed successfully",
                };
            } catch (insertError) {
                context.error("Error inserting data into BigQuery:", insertError.errors[0].errors, insertError.response.insertErrors[0].errors);

                return {
                    status: 500,
                    body: "Error inserting data into BigQuery",
                };
            }
        } catch (error) {
            context.error("Error processing Contentful Webhook:", error);
            return {
                status: 500,
                body: "Error processing Contentful Webhook",
            };
        }
    }
});