
# <p align="center">DVB-I Manager Plugin for Enigma2 (E²) ![GitHub repo size](https://img.shields.io/github/repo-size/oe-alliance-plugins/DvbIManager.svg)</p>

DVB-I service discovery, channel lists and broadcast/IP integration for OpenATV 8.0 and newer with the DVB-I Enigma2 core extensions installed.

This is an Enigma2 **Extension**, installed as `Plugins.Extensions.DvbIManager`. It is not a generic M3U/IPTV playlist generator.

## Features

- Discover live DVB-I service lists and select country, provider, language and region.
- Generate ordinary Enigma2 TV and radio bouquets without replacing unrelated bouquets.
- Use registered free-to-air IP alternatives when a broadcast tuner is busy or reception fails. Reception fallback stays on IP until the next channel change.
- Prefer native 4097 GStreamer playback in automatic mode; other installed players can be selected explicitly.
- Use Enigma2's picon paths and import available programme information, Now/Next and EPG.
- Run imports and updates as Enigma2 tasks, manually or through the standard scheduler, with toast notifications.
- Label VOD services when explicitly signalled by the service list or an already inspected manifest. Some public demo lists contain finite clips rather than live television.

DRM-protected services are not supported. The addon does not turn a DVB smartcard or CI/CI+ subscription into an IP DRM entitlement. Availability, codec support and geographical restrictions depend on the advertised services and receiver image.


## Github status
[![Build](https://github.com/oe-alliance-plugins/DvbIManager/actions/workflows/buildbot.yml/badge.svg)](https://github.com/oe-alliance-plugins/DvbIManager/actions/workflows/buildbot.yml)
[![Lint Status](https://github.com/oe-alliance-plugins/DvbIManager/actions/workflows/pylint.yml/badge.svg)](https://github.com/oe-alliance-plugins/DvbIManager/actions/workflows/pylint.yml)
[![Ruff Status](https://github.com/oe-alliance-plugins/DvbIManager/actions/workflows/ruff.yml/badge.svg)](https://github.com/oe-alliance-plugins/DvbIManager/actions/workflows/ruff.yml)
[![Build Status](https://github.com/oe-alliance-plugins/DvbIManager/actions/workflows/compile.yml/badge.svg)](https://github.com/oe-alliance-plugins/DvbIManager/actions/workflows/compile.yml)
[![AUTOTAG](https://github.com/oe-alliance-plugins/DvbIManager/actions/workflows/autotag.yml/badge.svg)](https://github.com/oe-alliance-plugins/DvbIManager/actions/workflows/autotag.yml)


[![Plugin Version](https://img.shields.io/github/v/tag/oe-alliance-plugins/DvbIManager?label=Latest%20Version&color=darkviolet)](https://github.com/oe-alliance-plugins/DvbIManager/tags)
[![Latest Release](https://img.shields.io/github/release-date/oe-alliance-plugins/DvbIManager?label=From&color=darkviolet)](https://github.com/oe-alliance-plugins/DvbIManager/releases/latest)
[![Github last commit](https://img.shields.io/github/last-commit/oe-alliance-plugins/DvbIManager)](https://github.com/oe-alliance-plugins/DvbIManager)
[![GitHub Activity](https://img.shields.io/github/commit-activity/y/oe-alliance-plugins/DvbIManager.svg?label=commits)](https://github.com/oe-alliance-plugins/DvbIManager/commits)
[![GitHub Activity](https://img.shields.io/github/commit-activity/m/oe-alliance-plugins/DvbIManager.svg?label=commits)](https://github.com/oe-alliance-plugins/DvbIManager/commits)

## SonarCloud status
[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=oe-alliance-plugins_DvbIManager&metric=alert_status)](https://sonarcloud.io/summary/new_code?id=oe-alliance-plugins_DvbIManager)
[![Vulnerabilities](https://sonarcloud.io/api/project_badges/measure?project=oe-alliance-plugins_DvbIManager&metric=vulnerabilities)](https://sonarcloud.io/summary/new_code?id=oe-alliance-plugins_DvbIManager)
[![Security Rating](https://sonarcloud.io/api/project_badges/measure?project=oe-alliance-plugins_DvbIManager&metric=security_rating)](https://sonarcloud.io/summary/new_code?id=oe-alliance-plugins_DvbIManager)
[![Bugs](https://sonarcloud.io/api/project_badges/measure?project=oe-alliance-plugins_DvbIManager&metric=bugs)](https://sonarcloud.io/summary/new_code?id=oe-alliance-plugins_DvbIManager)
[![Code Smells](https://sonarcloud.io/api/project_badges/measure?project=oe-alliance-plugins_DvbIManager&metric=code_smells)](https://sonarcloud.io/summary/new_code?id=oe-alliance-plugins_DvbIManager)
[![Duplicated Lines (%)](https://sonarcloud.io/api/project_badges/measure?project=oe-alliance-plugins_DvbIManager&metric=duplicated_lines_density)](https://sonarcloud.io/summary/new_code?id=oe-alliance-plugins_DvbIManager)
[![Reliability Rating](https://sonarcloud.io/api/project_badges/measure?project=oe-alliance-plugins_DvbIManager&metric=reliability_rating)](https://sonarcloud.io/summary/new_code?id=oe-alliance-plugins_DvbIManager)
[![Maintainability Rating](https://sonarcloud.io/api/project_badges/measure?project=oe-alliance-plugins_DvbIManager&metric=sqale_rating)](https://sonarcloud.io/summary/new_code?id=oe-alliance-plugins_DvbIManager)

[![SonarQube Cloud](https://sonarcloud.io/images/project_badges/sonarcloud-light.svg)](https://sonarcloud.io/summary/new_code?id=oe-alliance-plugins_DvbIManager)

---


## License

GPL-2.0-or-later. See [LICENSE.txt](LICENSE.txt). Translation extraction uses Enigma2's existing `xml2po.py`.

Report issues at [oe-alliance-plugins/DvbIManager](https://github.com/oe-alliance-plugins/DvbIManager/issues).
