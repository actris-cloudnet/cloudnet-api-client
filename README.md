# cloudnet-api-client

Python package for using Cloudnet API

## Quickstart

```python
import cloudnet_api_client as cac
client = cac.APIClient()
sites = client.sites(type="cloudnet")
products = client.products()
metadata = client.metadata("hyytiala", "2021-01-01", product=["mwr", "radar"])
cac.download(metadata, "data")
```

## License

MIT
