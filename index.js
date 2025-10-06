import * as dotenv from 'dotenv';
import PubSubApiClient from 'salesforce-pubsub-api-client';
import { createServer } from 'http';

async function run() {
    try {
        var response;
        const server = createServer((req, res) => {
            if (req.url === '/events') {
                res.writeHead(200, {
                    'Content-Type': 'text/event-stream',
                    'Cache-Control': 'no-cache',
                    'Connection': 'keep-alive',
                    'Access-Control-Allow-Origin': '*'
                });

                response = res;

                req.on('close', () => {
                });
            } else {
                res.writeHead(404);
                res.end();
            }
        });

        const PORT = 3000;
        server.listen(PORT, () => {
            console.log(`SSE server running at http://localhost:${PORT}/events`);
        });

        // Pub/Sub code starts here
        // Load config from .env file
        dotenv.config();

        // Build and connect Pub/Sub API client
        const client = new PubSubApiClient({
            pubSubEndpoint: 'api.pubsub.salesforce.com:443',
            authType: 'oauth-client-credentials',
            loginUrl: process.env.SALESFORCE_LOGIN_URL,
            clientId: process.env.SALESFORCE_CLIENT_ID,
            clientSecret: process.env.SALESFORCE_CLIENT_SECRET
        });
        await client.connect();

        // Prepare event callback
        const subscribeCallback = (subscription, callbackType, data) => {
            switch (callbackType) {
                case 'event':
                    const dataToSend = `data: ${JSON.stringify(data, (key, value) =>
                        typeof value === 'bigint' ? value.toString() : value
                    )}\n\n`;
                    response.write(dataToSend);
                    // Safely log event payload as a JSON string
                    console.log(
                        JSON.stringify(
                            data,
                            (key, value) =>
                                /* Convert BigInt values into strings and keep other types unchanged */
                                typeof value === 'bigint'
                                    ? value.toString()
                                    : value,
                            2
                        )
                    );
                    break;
                case 'lastEvent':
                    // Last event received
                    console.log(
                        `${subscription.topicName} - Reached last of ${subscription.requestedEventCount} requested event on channel. Closing connection.`
                    );
                    break;
                case 'end':
                    // Client closed the connection
                    console.log('Client shut down gracefully.');
                    break;
            }
        };

        // Subscribe to 3 account change event
        client.subscribe('/event/DCC_Agent_Events__e', subscribeCallback, 3);
    } catch (error) {
        console.error(error);
    }
}

run();