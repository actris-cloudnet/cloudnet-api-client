# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## 0.12.4 – 2025-08-26

- Add `software` attribute to `ProductMetadata`
- Fix hashability of `Product` in `ProductMetadata`
- Add possible timeliness values
- Use aware datetimes

## 0.12.3 – 2025-08-26

- Fix owners type

## 0.12.2 – 2025-08-25

- Fix product type filter bug

## 0.12.1 – 2025-08-25

- Fix tag subset filters

## 0.12.0 – 2025-08-25

- Adjust routes and responses

## 0.11.0 – 2025-08-16

- Adjust routes and responses
- Improve tests

## 0.10.0 – 2025-08-13

- Add `volatile` to metadata response
- Run CI tests against true dataportal backend

## 0.9.2 – 2025-08-04

- Use updated Cloudnet API

## 0.9.1 – 2025-06-19

- Support mobile sites without latitude and longitude
- Fix metadata fetching when `instrument_pid` defined

## 0.9.0 – 2025-05-14

- By default download to current folder

## 0.8.0 – 2025-05-14

- Add site to metadata responses

## 0.7.0 – 2025-05-06

- Validate checksum optionally
- Make site optional

## 0.6.0 – 2025-04-22

- Fix datetime parsing for Python 3.10
- Add Instrument and Model objects to metadata

## 0.5.1 – 2025-04-04

- Raise if downloading failed after retries
- Adjust logging

## 0.5.0 – 2025-04-04

- Add option to query one site
- Add undocumented `raw_model_metadata` function

## 0.4.1 – 2025-04-03

- Fix types

## 0.4.0 – 2025-04-03

- Move download function to APIClient class
- Add status parameter

## 0.3.0 – 2025-04-02

- Move `filename_prefix` and `filename_suffix` parameters
- Fix query logic

## 0.2.1 – 2025-04-02

- Return full paths of downloaded files

## 0.2.0 – 2025-04-01

- Add download progress bar
- Add `adownload` function for asynchronous context
- Only retry on aiohttp errors
- Extend date parameters
- Improve type hints

## 0.1.3 – 2025-03-31

- Add py.typed
- Add optimum model to metadata response
- Add model_id parameter

## 0.1.2 – 2025-03-27

- Add method to fetch instruments

## 0.1.1 – 2025-03-27

- Add `updated_at_from` and `updated_at_to` parameters

## 0.1.0 – 2025-03-27

- Initial release
