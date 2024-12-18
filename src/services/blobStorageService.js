const { BlobServiceClient } = require("@azure/storage-blob");

class BlobStorageService {
  #client;
  #logger;

  constructor(options) {
    const { accountName, logger, credential } = this.validate(options);
    const blobURL = `https://${accountName}.blob.core.windows.net`;
    this.#client = new BlobServiceClient(blobURL, credential);
    this.#logger = logger;
  }

  validate(options) {
    if (!options.accountName) {
      throw new Error("Azure Storage Account Name not found");
    }

    return options;
  }

  async upload(containerName, blobName, data) {
    try {
      this.logger.log("creating log...");
      const container = this.client.getContainerClient(containerName);
      const blockBlobClient = container.getBlockBlobClient(blobName);
      const response = await blockBlobClient.upload(data, data.length);
      return [true, response.requestId];
    } catch (error) {
      this.logger.error(`Error creating log: ${containerName}/${blobName}::${data}`, error);
      return [false, null];
    }
  }

  get client() {
    return this.#client;
  }

  get logger() {
    return this.#logger;
  }
}

module.exports = BlobStorageService;
