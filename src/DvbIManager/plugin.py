# -*- coding: utf-8 -*-
#
# DVB-I Manager for OpenATV / Enigma2
#
# Phase 1: service list import into normal bouquets.
#

from collections import deque
from gettext import dgettext
from os.path import exists as pathExists, isfile, join, normcase, normpath
from time import localtime, mktime, time
from urllib.parse import unquote
from weakref import ref as weakRef

from Components.ActionMap import HelpableActionMap
from Components.International import international
from Components.Renderer import LcdPicon, Picon
from Components.Sources.StaticText import StaticText
from Components.Task import Job, PythonTask, Task, job_manager as jobManager
from Components.config import ConfigSelection, ConfigSubsection, ConfigText, ConfigYesNo, config, configfile
from Plugins.Plugin import PluginDescriptor
from Screens.Setup import Setup
from Screens.TextBox import TextBox
from Scheduler import SchedulerEntry, TIMERTYPE, addFunctionTimer, functionTimers

from enigma import eDVBDB, eEPGCache, eServiceReference, eTimer, getDesktop, setDVBIFallbackServices

from . import _, __version__, PLUGIN_DOMAIN
from .DvbI import DvbIManager, REGISTRY_CATALOG_SCHEMA, loadSourceConfig, getRegistrySourceList, runSync, getAvailablePlayers, installBouquets, refreshPicons, regionCatalogPath, withRegionId, atomicWriteJson, readJson


PLUGIN_VERSION = __version__

SCHEDULER_ID = "dvbi_update"
SESSION = None

IP_SERVICE_TYPE_CHOICES = [
	("auto", _("Automatic (4097 GStreamer preferred)")),
	("4097", _("4097 GStreamer / Enigma2")),
	("5002", _("5002 ServiceApp / exteplayer3")),
	("1", _("1 native HTTP MPEG-TS only")),
	("5001", _("5001 ServiceApp gstplayer")),
]


config.plugins.dvbi = ConfigSubsection()
config.plugins.dvbi.enabled = ConfigYesNo(default=True)
config.plugins.dvbi.advanced = ConfigYesNo(default=False)
config.plugins.dvbi.initial_selection_done = ConfigYesNo(default=False)
config.plugins.dvbi.cache_dir = ConfigText(default="/media/hdd/dvbi", fixed_size=False)
config.plugins.dvbi.registry_country = ConfigSelection(default="", choices=[("", _("All countries"))])
config.plugins.dvbi.registry_provider = ConfigSelection(default="", choices=[("", _("All providers"))])
config.plugins.dvbi.registry_language = ConfigSelection(default="", choices=[("", _("All languages"))])
config.plugins.dvbi.registry_delivery = ConfigSelection(default="", choices=[("", _("All delivery systems"))])
config.plugins.dvbi.registry_offering_id = ConfigSelection(default="", choices=[("", _("Select one live service list"))])
config.plugins.dvbi.region = ConfigSelection(default="", choices=[("", _("Default / no regional selection"))])
config.plugins.dvbi.postcode = ConfigText(default="", fixed_size=False)
config.plugins.dvbi.prefer_broadcast = ConfigYesNo(default=True)
config.plugins.dvbi.hybrid = ConfigYesNo(default=True)
config.plugins.dvbi.ip_service_type = ConfigSelection(default="auto", choices=IP_SERVICE_TYPE_CHOICES)
config.plugins.dvbi.download_logos = ConfigYesNo(default=True)
config.plugins.dvbi.export_xmltv = ConfigYesNo(default=False)
config.plugins.dvbi.export_epg_probe = ConfigYesNo(default=True)
config.plugins.dvbi.sync_epg_now_next = ConfigYesNo(default=True)
config.plugins.dvbi.xmltv_path = ConfigText(default="/media/hdd/dvbi/metadata/dvbi_channels.xml", fixed_size=False)

# Keep the stored choice valid until the live catalogue supplies its label.
# ConfigSelection.load() deliberately maps values outside its choices to default.
for configElement in (
	config.plugins.dvbi.registry_country,
	config.plugins.dvbi.registry_provider,
	config.plugins.dvbi.registry_language,
	config.plugins.dvbi.registry_delivery,
	config.plugins.dvbi.registry_offering_id,
	config.plugins.dvbi.region,
):
	savedValue = configElement.getSavedValue()
	if savedValue:
		configElement.setChoices(configElement.getSelectionList() + [(savedValue, savedValue + _(" (saved)"))], default="")
		configElement.load()


def metadataDirectories():
	configured = normpath(config.plugins.dvbi.cache_dir.value or DvbIManager.DEFAULT_DATA_DIR)
	candidates = [join(configured, "metadata"), join(DvbIManager.FALLBACK_DATA_DIR, "metadata")]
	result = []
	for candidate in candidates:
		normalized = normcase(normpath(candidate))
		if normalized not in [normcase(normpath(item)) for item in result]:
			result.append(candidate)
	return result


def registryEndpoint(value=None):
	return str(value or loadSourceConfig()["sources"][0]["url"]).strip()


def getRegistrySources():
	return getRegistrySourceList(registryEndpoint())


def registryCatalogState(now=None):
	"""Return the newest usable live catalogue and its freshness state."""
	now = int(time() if now is None else now)
	try:
		endpoint = registryEndpoint()
		sources = getRegistrySources()
	except ValueError as error:
		return {"state": "error", "path": "", "error": str(error), "data": {"offerings": [], "facets": {}}}
	candidates = []
	for metadataDir in metadataDirectories():
		path = join(metadataDir, "registry_catalog.json")
		data = readJson(path, default={})
		if (
			not isinstance(data, dict)
			or data.get("schema") != REGISTRY_CATALOG_SCHEMA
			or data.get("query_scope") != "all"
			or not isinstance(data.get("offerings"), list)
			or not data.get("offerings")
		):
			continue
		if data.get("registry_url") and str(data["registry_url"]).rstrip("/").casefold() != str(endpoint or "").rstrip("/").casefold():
			continue
		try:
			updatedAt = int(data.get("updated_at", 0) or 0)
			expiresAt = int(data.get("expires_at", 0) or 0)
		except (TypeError, ValueError):
			updatedAt = 0
			expiresAt = 0
		fresh = bool(data.get("registry_sources") == sources and updatedAt and (updatedAt >= now or expiresAt > now))
		candidates.append((updatedAt, fresh, path, data))

	if not candidates:
		return {"state": "missing", "path": "", "data": {"offerings": [], "facets": {}}}
	candidates.sort(key=lambda item: (item[0], item[1]), reverse=True)
	unusedUpdatedAt, fresh, path, data = candidates[0]
	return {"state": "fresh" if fresh else "stale", "path": path, "data": data}


def readRegistryCatalog():
	return registryCatalogState()["data"]


def offeringKey(offering):
	return str(offering.get("selection_id") or offering.get("id") or offering.get("url") or "")


def selectedRegistryOffering(catalog=None):
	selected = str(config.plugins.dvbi.registry_offering_id.value or "")
	for offering in (catalog or readRegistryCatalog()).get("offerings", []):
		if isinstance(offering, dict) and offeringKey(offering) == selected:
			return offering
	return None


def selectedRegistryUrl(catalog=None):
	offering = selectedRegistryOffering(catalog=catalog)
	if not offering:
		raise ValueError(_("Select one service list from the live DVB-I registry."))
	urls = offering.get("urls") or [offering.get("url")]
	url = next((item for item in urls if item), "")
	if not url:
		raise ValueError(_("The selected live registry entry has no usable ServiceListURI."))
	return url


def regionCatalogExists(url, maxAge=86400):
	now = int(time())
	for metadataDir in metadataDirectories():
		path = regionCatalogPath(metadataDir, url)
		data = readJson(path, default={})
		if not isinstance(data, dict) or data.get("schema") != "org.openatv.dvbi.region-catalog.v1":
			continue
		try:
			updatedAt = int(data.get("updated_at", 0))
		except (TypeError, ValueError):
			updatedAt = 0
		if updatedAt and (updatedAt >= now or now - updatedAt <= int(maxAge)):
			return True
	return False


def broadcastServiceSnapshot():
	"""Take a fast in-process snapshot; background tasks must not access eDVBDB."""
	from Components.NimManager import nimmanager

	database = eDVBDB.getInstance()
	enabled = {kind: bool(nimmanager.getEnabledNimListOfType(kind)) for kind in ("DVB-S", "DVB-C", "DVB-T")}
	satellites = set(nimmanager.getConfiguredSats()) if enabled["DVB-S"] else set()
	result = []
	for reference, metadata in database.getAllServicesRaw().items():
		ref = eServiceReference(reference)
		position = ref.getUnsignedData(4) >> 16
		kind = "DVB-T" if position == 0xEEEE else "DVB-C" if position == 0xFFFF else "DVB-S"
		if not enabled[kind] or kind == "DVB-S" and position not in satellites:
			continue
		result.append({"ref": reference, "name": metadata[0], "crypted": bool(database.isCrypted(ref))})
	return result


def importOptions(force):
	if config.picon.mode.value:
		piconPaths = [
			Picon.getPiconPath("infobar"),
			Picon.getPiconPath("channelselection"),
			LcdPicon.getPiconPath(),
			getattr(config.picon, f"set{config.picon.openwebif.value}").path.value,
		]
	else:
		# Single-path mode uses the renderers' discovered paths, not the inactive multi-path assignments.
		piconPaths = [path for path in Picon.searchPaths + LcdPicon.searchPaths if not path.startswith("/media/net")]
	options = {
		"country": config.plugins.dvbi.registry_country.value,
		# Empty is the deliberate "All languages" value.  The parser then
		# accepts all language variants and applies its normal document/root
		# fallback; never silently replace it with an offering's first code.
		"language": config.plugins.dvbi.registry_language.value if config.plugins.dvbi.advanced.value else "",
		# A postcode sent to a list server is not necessarily a RegionIdRef.
		# Only the explicit region identifier may select/filter DVB-I regions.
		"region": config.plugins.dvbi.region.value,
		"show_other_regions": False,
		"include_ip": True,
		"prefer_broadcast": config.plugins.dvbi.prefer_broadcast.value,
		"hybrid": bool(config.plugins.dvbi.enabled.value and config.plugins.dvbi.hybrid.value),
		"ip_service_type": config.plugins.dvbi.ip_service_type.value,
		"automatic_player": config.plugins.dvbi.ip_service_type.value == "auto",
		"available_players": getAvailablePlayers(),
		"native_bouquets": True,
		# DVB-I imports are FTA-only.
		"include_drm": False,
		"download_logos": config.plugins.dvbi.download_logos.value,
		"install_picons": config.plugins.dvbi.download_logos.value,
		"picon_dirs": list(dict.fromkeys(normpath(path) for path in piconPaths if path)),
		"export_xmltv": config.plugins.dvbi.export_xmltv.value,
		"export_epg_probe": config.plugins.dvbi.export_epg_probe.value,
		"sync_epg_now_next": config.plugins.dvbi.sync_epg_now_next.value,
		"epg_workers": 4,
		"probe_media": True,
		"media_probe_max": 1024,
		"inspect_manifests": True,
		"manifest_max": 1024,
		"xmltv_path": config.plugins.dvbi.xmltv_path.value,
		"force": bool(force),
		# A manually forced run explicitly confirms a legitimate provider-side
		# removal of more than half of a previously valid service list.
		"allow_large_removal": bool(force),
	}
	options["service_snapshot"] = broadcastServiceSnapshot()
	options["require_fta_verification"] = True
	return options


def buildImportJob(force=False):
	"""Snapshot settings and receiver services on the E2 main loop."""
	options = importOptions(force)
	options["label_vod"] = bool((selectedRegistryOffering() or {}).get("test_source"))
	return {
		"action": "import_url",
		"data_dir": config.plugins.dvbi.cache_dir.value,
		"enigma2_dir": "/etc/enigma2",
		"url": withRegionId(selectedRegistryUrl(), config.plugins.dvbi.region.value),
		"options": options,
		"force": bool(force),
	}


def buildRegionJob():
	url = selectedRegistryUrl()
	if regionCatalogExists(url):
		return None
	return {
		"action": "discover_regions",
		"data_dir": config.plugins.dvbi.cache_dir.value,
		"url": url,
		"options": {
			"country": config.plugins.dvbi.registry_country.value,
			"language": config.plugins.dvbi.registry_language.value if config.plugins.dvbi.advanced.value else "",
		},
	}


def buildRegistryJob(force=False):
	"""Build one live selector catalogue from global and national registries."""
	return {
		"action": "discover_registry",
		"data_dir": config.plugins.dvbi.cache_dir.value,
		"enigma2_dir": "/etc/enigma2",
		"registry_url": registryEndpoint(),
		"registry_sources": getRegistrySources(),
		# The selector catalogue is small receiver configuration state.  Keep
		# it persistent even when bulk media/cache data is intentionally put
		# below /tmp or removable storage.
		"catalog_dir": join(DvbIManager.FALLBACK_DATA_DIR, "metadata"),
		"force": bool(force),
	}


class DvbISyncTask(PythonTask):
	"""Standard E2 background task; only plain Python data crosses threads."""

	def __init__(self, job, request):
		PythonTask.__init__(self, job, _("Download and prepare DVB-I data"))
		self.request = request
		self.message = ""
		self.weighting = 95

	def _run(self):
		PythonTask._run(self)
		# Network stages do not need the PythonTask default 5 ms progress timer.
		self.timer.start(250)

	def work(self):
		self.report(_("Loading DVB-I data"))
		self.job.result = runSync(self.request, logger=self.report)
		self.report(_("DVB-I data prepared"))
		self.job.result.update(ok=True, finished_at=int(time()))
		self.pos = 100

	def abort(self):
		# Queued jobs have no Task callback yet; the standard queue starts them later.
		self.job.cancelRequested = True
		if hasattr(self, "callback"):
			PythonTask.abort(self)

	def report(self, message):
		if self.aborted or self.job.cancelRequested:
			raise RuntimeError(_("DVB-I update cancelled"))
		self.message = str(message)

	def onComplete(self, result):
		# PythonTask receives a Twisted Failure on exceptions; keep toasts concise.
		if hasattr(result, "getErrorMessage"):
			result = RuntimeError(result.getErrorMessage())
		PythonTask.onComplete(self, result)

	def onTimer(self):
		PythonTask.onTimer(self)
		if self.message:
			notifySetupScreens("taskProgress", self.message)
			self.message = ""
		if self.job.schedulerEntry is not None:
			self.job.schedulerEntry.functionProgress = self.job.progress

	def cleanup(self, failed):
		self.request = None


class DvbIPublishTask(Task):
	"""Publish through E2 on the main loop, with bounded EPG batches."""

	def __init__(self, job):
		Task.__init__(self, job, _("Update channel lists and programme guide"))
		self.weighting = 5
		self.queue = deque()
		self.timer = None
		self.total = 0

	def _run(self):
		if self.job.cancelRequested:
			raise RuntimeError(_("DVB-I update cancelled"))
		reloadBouquets(self.job.result)
		PLAYBACK_CACHE.clear()
		self.queue.extend(self.job.result.pop("epg_events", []))
		self.total = len(self.queue)
		self.timer = eTimer()
		self.timer.callback.append(self.runBatch)
		self.runBatch()

	def runBatch(self):
		try:
			cache = eEPGCache.getInstance()
			if self.queue and cache is None:
				raise RuntimeError(_("Enigma2 EPG cache is unavailable"))
			for unused in range(min(8, len(self.queue))):
				entry = self.queue.popleft()
				if entry.get("service_reference") and entry.get("import_events"):
					cache.importEvents(entry["service_reference"], entry["import_events"])
			self.setProgress(100 if not self.total else 100 * (self.total - len(self.queue)) // self.total)
		except Exception as error:
			from Components.Task import FailedPostcondition

			self.postconditions.append(FailedPostcondition(error))
			self.finish()
			return
		if self.queue:
			self.timer.start(10, True)
		else:
			self.finish()

	def cleanup(self, failed):
		if self.timer is not None:
			self.timer.stop()
		self.queue.clear()


PLAYBACK_CACHE = {}


def activateHybrid(result=None, shutdown=False):
	"""Publish a small, already-probed map; core never reads plugin settings."""
	if shutdown or not (config.plugins.dvbi.enabled.value and config.plugins.dvbi.hybrid.value and config.plugins.dvbi.prefer_broadcast.value):
		setDVBIFallbackServices([])
		return
	try:
		cached = {}
		for directory in metadataDirectories():
			path = join(directory, "hybrid_map.json")
			if isfile(path):
				cached = readJson(path, default={})
				break
		if cached and cached.get("version") != 2:
			raise ValueError(_("Invalid DVB-I fallback cache version; create the channel list again"))
		lists = cached.get("lists", {})
		if not isinstance(lists, dict):
			raise ValueError(_("Invalid DVB-I per-list fallback cache"))
		lists = dict(lists)
		if result is not None and "hybrid_services" in result:
			key = result.get("bouquet_tv")
			if not isinstance(key, str) or not key.startswith("userbouquet.dvbi_") or not key.endswith(".tv") or "/" in key or "\\" in key:
				raise ValueError(_("Missing DVB-I channel-list identity"))
			# Replace only this list, including a deliberately empty mapping.
			lists[key] = result["hybrid_services"]
		pairs = []
		for services in lists.values():
			if not isinstance(services, list):
				raise ValueError(_("Invalid DVB-I per-list fallback entries"))
			pairs.extend(services)
		# Ambiguous cross-list matches must not silently choose another programme.
		mapping = {}
		conflicts = set()
		for source, target in pairs:
			if source in mapping and mapping[source] != target:
				conflicts.add(source)
			mapping[source] = target
		pairs = [(source, target) for source, target in mapping.items() if source not in conflicts]
		count = setDVBIFallbackServices(pairs)
		if count < 0:
			raise ValueError(_("Invalid DVB-I fallback map"))
		if result is not None:
			manager = DvbIManager(dataDir=config.plugins.dvbi.cache_dir.value)
			atomicWriteJson(join(manager.metadataDir, "hybrid_map.json"), {"version": 2, "lists": lists})
			result["hybrid_services_active"] = count
	except Exception as error:
		if result is not None:
			# A rejected new import must not discard other committed lists.
			activateHybrid()
		else:
			setDVBIFallbackServices([])
		print("[DvbIManager] Hybrid playback update failed: {0}".format(error))


def reloadBouquets(result):
	if result.get("native_bouquets") is not None and not result.get("bouquets_committed"):
		database = eDVBDB.getInstance()
		installBouquets(result["native_bouquets"], database, eServiceReference)
		result["bouquets_committed"] = True
		result.pop("native_bouquets", None)
		result["needs_bouquet_reload"] = False
		activateHybrid(result)
		refreshPicons()
		from Components.ServiceList import refreshServiceList

		refreshServiceList()
		return
	if not result.get("needs_bouquet_reload"):
		return
	try:
		database = eDVBDB.getInstance()
		if database:
			database.reloadBouquets()
	except Exception as error:
		print("[DvbIManager] Bouquet reload failed: {0}".format(error))


def resolveDvbiService(service, **kwargs):
	"""Resolve a stable dvbi:// token without doing network I/O on channel zap."""

	def playbackEntry(token):
		if token in PLAYBACK_CACHE:
			return PLAYBACK_CACHE[token]
		configured = config.plugins.dvbi.cache_dir.value or DvbIManager.DEFAULT_DATA_DIR
		paths = [
			join(configured, "metadata", "playback", token + ".json"),
			join(DvbIManager.FALLBACK_DATA_DIR, "metadata", "playback", token + ".json"),
		]
		path = next((item for item in paths if pathExists(item)), "")
		entry = readJson(path, default={}) if path else {}
		if len(PLAYBACK_CACHE) >= 128:
			PLAYBACK_CACHE.pop(next(iter(PLAYBACK_CACHE)))
		PLAYBACK_CACHE[token] = entry
		return entry

	if not service:
		return None, None
	try:
		path = unquote(service.getPath() or "")
	except Exception:
		path = ""
	if not path.startswith("dvbi://"):
		return None, None
	token = path[len("dvbi://"):].split("/", 1)[0]
	entry = playbackEntry(token)
	url = entry.get("url", "")
	if url:
		return url, None
	return None, _("No cached playback target exists for DVB-I service {0}").format(token)


def stringValues(values):
	result = []
	for value in values or []:
		value = str(value or "").strip()
		if value and value.casefold() not in [item.casefold() for item in result]:
			result.append(value)
	return sorted(result, key=lambda item: item.casefold())


def setSelectionChoices(element, choices, desired=None, savedLabel=_("saved")):
	"""Update a persistent ConfigSelection without replacing its identity."""
	normalized = []
	seen = set()
	for value, label in choices:
		value = str(value or "")
		if value in seen:
			continue
		seen.add(value)
		normalized.append((value, str(label or value)))

	saved = element.getSavedValue() or element.default
	current = str(element.value or "") if desired is None else str(desired or "")
	for value in (saved, current):
		if value and value not in seen:
			seen.add(value)
			normalized.append((value, "{0} ({1})".format(value, savedLabel)))

	if not normalized or normalized[0][0] != "":
		normalized.insert(0, ("", _("All")))
	element.setChoices(normalized, default="")
	if current in [item[0] for item in normalized]:
		element.value = current
	return str(element.value or "")


def offeringMatches(offering, country="", provider="", language="", delivery=""):
	countries = stringValues(offering.get("target_countries"))
	languages = stringValues(offering.get("languages"))
	deliveries = stringValues(offering.get("delivery"))
	if country and countries and country.casefold() not in [item.casefold() for item in countries]:
		return False
	if provider and str(offering.get("provider") or "").casefold() != provider.casefold():
		return False
	if language and languages and language.casefold() not in [item.casefold() for item in languages]:
		return False
	if delivery and deliveries and delivery.casefold() not in [item.casefold() for item in deliveries]:
		return False
	return True


def offeringLabel(offering):
	provider = str(offering.get("provider") or "").strip()
	name = str(offering.get("name") or offering.get("url") or _("Unnamed list")).strip()
	countries = ",".join(stringValues(offering.get("target_countries")))
	languages = ",".join(stringValues(offering.get("languages")))
	label = "{0} — {1}".format(provider, name) if provider and provider != name else name
	if offering.get("test_source"):
		label += _(" (Test / Demo)")
	if offering.get("registry_cache_stale"):
		label += _(" (cached; source unavailable)")
	if not config.plugins.dvbi.advanced.value:
		return label
	qualifiers = [value for value in (countries, languages) if value]
	return "{0} [{1}]".format(label, " / ".join(qualifiers)) if qualifiers else label


def applyInitialSelection(offerings, changed=None):
	"""One-time live German production default, never an automatic import."""
	dvbi = config.plugins.dvbi
	if dvbi.initial_selection_done.value:
		return
	selectors = (dvbi.registry_country, dvbi.registry_provider, dvbi.registry_language, dvbi.registry_delivery, dvbi.registry_offering_id, dvbi.region)
	if changed in selectors or any(element.value for element in selectors):
		# Existing installations and user edits always win, including choosing
		# All countries or clearing the list while discovery is still running.
		dvbi.initial_selection_done.value = True
		dvbi.initial_selection_done.save()
		return
	try:
		defaults = loadSourceConfig()
	except ValueError:
		return  # The setup reports the source-file error, without static fallbacks.
	country = defaults["default_country"]
	if not country:
		return
	candidates = [
		item
		for item in offerings
		if country in item.get("target_countries", [])
		and item.get("regulator_list")
		and not item.get("test_source")
		and offeringKey(item)
		and (item.get("url") or any(item.get("urls") or []))
	]
	if not candidates:
		return  # Discovery can finish later; never substitute a demo list.
	candidates.sort(key=lambda item: item.get("registry_url") != defaults["default_registry"])
	offering = candidates[0]
	for element, value, label in (
		(dvbi.registry_country, country, country),
		(dvbi.registry_offering_id, offeringKey(offering), offeringLabel(offering)),
		(dvbi.region, defaults["default_region"], defaults["default_region"]),
	):
		setSelectionChoices(element, [("", ""), (value, label)], desired=value)
		element.save()
	dvbi.initial_selection_done.value = True
	dvbi.initial_selection_done.save()
	configfile.save()


def refreshRegistryConfigChoices(changed=None):
	"""Hydrate setup.xml selectors exclusively from the persistent live SLR cache."""
	dvbi = config.plugins.dvbi
	catalog = readRegistryCatalog()
	offerings = [item for item in catalog.get("offerings", []) if isinstance(item, dict)]
	facets = catalog.get("facets", {}) if isinstance(catalog.get("facets"), dict) else {}
	applyInitialSelection(offerings, changed)

	if changed is dvbi.registry_country:
		dvbi.registry_provider.value = ""
		dvbi.registry_language.value = ""
		dvbi.registry_delivery.value = ""
		dvbi.registry_offering_id.value = ""
	elif changed is dvbi.registry_provider:
		dvbi.registry_language.value = ""
		dvbi.registry_delivery.value = ""
		dvbi.registry_offering_id.value = ""
	elif changed in (dvbi.registry_language, dvbi.registry_delivery):
		dvbi.registry_offering_id.value = ""
	elif changed is dvbi.registry_offering_id and dvbi.registry_offering_id.value:
		dvbi.region.value = ""

	countries = facets.get("target_countries") or [value for item in offerings for value in item.get("target_countries", [])]
	countryNames = international.getNIMCountries()
	country = setSelectionChoices(
		dvbi.registry_country,
		[("", _("All countries"))] + [(value, dgettext("enigma2", countryNames.get(value, value))) for value in stringValues(countries)],
	)

	countryScope = [item for item in offerings if offeringMatches(item, country=country)]
	providers = [item.get("provider", "") for item in countryScope]
	provider = setSelectionChoices(
		dvbi.registry_provider,
		[("", _("All providers"))] + [(value, value) for value in stringValues(providers)],
	)

	providerScope = [item for item in countryScope if offeringMatches(item, provider=provider if dvbi.advanced.value else "")]
	languages = [value for item in providerScope for value in item.get("languages", [])]
	language = setSelectionChoices(
		dvbi.registry_language,
		[("", _("All languages"))] + [(value, value) for value in stringValues(languages)],
	)

	languageScope = [item for item in providerScope if offeringMatches(item, language=language if dvbi.advanced.value else "")]
	deliveries = [value for item in languageScope for value in item.get("delivery", [])]
	delivery = setSelectionChoices(
		dvbi.registry_delivery,
		[("", _("All delivery systems"))] + [(value, value) for value in stringValues(deliveries)],
	)

	filtered = [item for item in languageScope if offeringMatches(item, delivery=delivery if dvbi.advanced.value else "")]
	filtered.sort(key=lambda item: not item.get("regulator_list") or bool(item.get("test_source")))
	emptyLabel = _("Select one live service list") if offerings else _("Live registry data not loaded")
	offeringChoices = [("", emptyLabel)]
	offeringChoices.extend((offeringKey(item), offeringLabel(item)) for item in filtered if offeringKey(item))
	currentOffering = str(dvbi.registry_offering_id.value or "")
	currentItem = next((item for item in offerings if offeringKey(item) == currentOffering), None)
	if currentItem and currentOffering not in [item[0] for item in offeringChoices]:
		offeringChoices.append((currentOffering, offeringLabel(currentItem) + _(" (selected; outside filters)")))
	offeringId = setSelectionChoices(
		dvbi.registry_offering_id,
		offeringChoices,
		desired=currentOffering,
		savedLabel=_("saved; unavailable in current live catalogue"),
	)

	selected = next((item for item in offerings if offeringKey(item) == offeringId), None)
	selectedUrls = (selected or {}).get("urls") or [(selected or {}).get("url")]
	selectedUrl = next((url for url in selectedUrls if url), "")
	regions = []
	if selectedUrl:
		for metadataDir in metadataDirectories():
			regionData = readJson(regionCatalogPath(metadataDir, selectedUrl), default={})
			if isinstance(regionData, dict) and isinstance(regionData.get("regions"), list):
				regions = regionData["regions"]
				break
	regionChoices = [("", _("Default / no regional selection"))]
	regionChoices.extend(
		(
			str(item.get("region_id")),
			str(item.get("display_path") or item.get("name") or item.get("region_id")),
		)
		for item in regions
		if isinstance(item, dict) and item.get("region_id") and item.get("selectable", True)
	)
	setSelectionChoices(dvbi.region, regionChoices, savedLabel=_("saved; region data not loaded"))
	return {
		"offerings": len(offerings),
		"countries": len(stringValues(countries)),
		"providers": len(stringValues(providers)),
		"languages": len(stringValues(languages)),
		"delivery": len(stringValues(deliveries)),
		"regions": len(regionChoices) - 1,
	}


# Disk cache only: this performs no network I/O during plugin import.
refreshRegistryConfigChoices()


SETUP_SCREENS = []


def notifySetupScreens(method, *args):
	def liveSetupScreens():
		live = []
		retained = []
		for reference in SETUP_SCREENS:
			screen = reference()
			if screen is not None and getattr(screen, "uiActive", False):
				live.append(screen)
				retained.append(reference)
		SETUP_SCREENS[:] = retained
		return live

	for screen in liveSetupScreens():
		try:
			getattr(screen, method)(*args)
		except Exception as error:
			print("[DvbIManager] Setup notification failed: {0}".format(error))


def formatTaskResult(result):
	if not result.get("ok"):
		return _("DVB-I synchronisation failed:\n{0}").format(result.get("error", _("unknown task error")))
	action = result.get("action")
	if action == "discover_registry":
		text = _("Available channel lists updated: {0}. Choose a list and, if offered, your region.").format(result.get("offerings_total", 0))
		if result.get("source_errors"):
			text += _("\nSome sources are unavailable; their last usable entries were retained.")
			text += "\n" + "\n".join("{0}: {1}".format(item["url"], item["error"]) for item in result["source_errors"])
		return text
	if not config.plugins.dvbi.advanced.value:
		if action == "discover_regions":
			return _("Regions loaded for {0}.").format(result.get("service_list_name", ""))
		text = _("Channel list updated: {0}\n{1} TV channels, {2} radio channels.\nThe channels are available in the normal TV and radio lists.").format(
			result.get("service_list_name", ""), result.get("services_written_tv", 0), result.get("services_written_radio", 0)
		)
		if result.get("http_stale"):
			text += _("\nThe provider could not supply a valid update; the last usable list was used.")
		if result.get("epg_services_failed") or result.get("epg_services_truncated"):
			text += _("\nProgramme information is incomplete; check the import report.")
		return text
	if action == "discover_regions":
		return (_("DVB-I regions loaded.\nList: {0}\nRegions: {1} selectable, {2} total")).format(
			result.get("service_list_name", ""),
			result.get("regions_selectable", 0),
			result.get("regions_total", 0),
		)
	bouquetNames = []
	if result.get("services_written_tv", 0) and result.get("bouquet_tv"):
		bouquetNames.append(_("TV: ") + result.get("bouquet_tv", ""))
	if result.get("services_written_radio", 0) and result.get("bouquet_radio"):
		bouquetNames.append(_("Radio: ") + result.get("bouquet_radio", ""))
	return (
		_(
			"DVB-I import complete.\n"
			"List: {0}\n"
			"Services: {1}, written: {2}, skipped: {3}\n"
			"Written TV/radio: {4}/{5}\n"
			"Broadcast match/written: {6}/{7}, IP written: {8}\n"
			"EPG mapped/schedule sources: {9}/{10}\n"
			"Bouquets: {11}\n"
			"Report: {12}"
		)
	).format(
		result.get("service_list_name", ""),
		result.get("services_total", 0),
		result.get("services_written", 0),
		result.get("services_skipped", 0),
		result.get("services_written_tv", 0),
		result.get("services_written_radio", 0),
		result.get("services_matched_broadcast", 0),
		result.get("services_written_broadcast", 0),
		result.get("services_written_ip", 0),
		result.get("epg_services_mapped", 0),
		result.get("epg_services_with_schedule_endpoint", 0),
		", ".join(bouquetNames),
		result.get("report_path", ""),
	)


def showTaskResult(session, result):
	"""Use the session's non-blocking toast queue, never a task popup."""
	try:
		if not result.get("ok"):
			print("[DvbIManager] {0}".format(formatTaskResult(result)))
			detail = " ".join(str(result.get("error") or _("unknown task error")).split())
			if len(detail) > 180:
				detail = detail[:177] + "..."
			session.showError(_("DVB-I update failed: {0}").format(detail), timeout=8)
			return
		action = result.get("action")
		if action == "discover_registry":
			text = _("DVB-I: {0} available channel lists updated.").format(result.get("offerings_total", 0))
		elif action == "discover_regions":
			text = _("DVB-I: Regions for the selected channel list updated.")
		else:
			text = _("DVB-I: {0}{1} — {2} TV, {3} radio.").format(
				result.get("service_list_name", _("Channel list updated")),
				" (IP)" if result.get("prefer_broadcast") is False else "",
				result.get("services_written_tv", 0),
				result.get("services_written_radio", 0),
			)
		if not result.get("sync_complete", True):
			session.showWarning(text + _(" Update incomplete; please check the last result."), timeout=8)
		elif result.get("http_stale"):
			session.showWarning(text + _(" Provider unavailable; cached data used."), timeout=8)
		else:
			session.showInfo(text, timeout=6)
	except Exception as error:
		print("[DvbIManager] Could not display task result: {0}".format(error))


def dvbiJobs():
	return [job for job in jobManager.getPendingJobs() if hasattr(job, "dvbiAction") and job.status in (Job.NOT_STARTED, Job.IN_PROGRESS)]


def taskRunning():
	return bool(dvbiJobs())


def launchTask(request, session, notify=False, callback=None, schedulerEntry=None):
	identity = (request["action"], request.get("url", ""))
	if any((job.dvbiAction, job.dvbiUrl) == identity for job in dvbiJobs()):
		return False
	job = Job(_("DVB-I channel update"))
	job.dvbiAction, job.dvbiUrl = identity
	job.result = {"action": request["action"]}
	job.cancelRequested = False
	job.schedulerEntry = schedulerEntry
	DvbISyncTask(job, request)
	DvbIPublishTask(job)

	def finished(completed, task=None, problems=()):
		result = completed.result
		if problems:
			result.update(ok=False, error="; ".join(problem.getErrorMessage(task) for problem in problems))
		result.pop("epg_events", None)
		backgroundJobFinished(result, session, notify)
		success = bool(result.get("ok") and result.get("sync_complete", True))
		if schedulerEntry is not None:
			schedulerEntry.log(0 if success else 30, formatTaskResult(result))
		if callback is not None:
			callback(success)
		return False  # E2 JobManager records the failure; no modal retry popup.

	jobManager.AddJob(job, onSuccess=finished, onFail=finished)
	return True


def backgroundJobFinished(result, session, notify=False):
	if result.get("ok") and result.get("action") in ("discover_registry", "discover_regions"):
		refreshRegistryConfigChoices()
	notifySetupScreens("taskFinished", result)
	if notify:
		showTaskResult(session, result)
	elif not result.get("ok"):
		print("[DvbIManager] {0}".format(formatTaskResult(result).replace("\n", " ")))


def requestRegistryRefresh(session, force=False, notify=False):
	state = registryCatalogState()
	if state["state"] == "error":
		session.showError(state["error"], timeout=8)
		return False
	if not force and state["state"] == "fresh":
		refreshRegistryConfigChoices()
		return False
	return launchTask(buildRegistryJob(force=force), session, notify=notify)


def requestSelectedImport(session, force=True):
	if not launchTask(buildImportJob(force=force), session, notify=True):
		raise RuntimeError(_("A DVB-I task is already running."))
	return "import_started"


def startScheduled(callback, entry):
	"""Scheduler's asynchronous entry point; JobManager owns execution."""
	if SESSION is None or not config.plugins.dvbi.enabled.value:
		entry.log(30, _("DVB-I Manager is disabled or not ready."))
		return False
	try:
		started = launchTask(buildImportJob(), SESSION, notify=True, callback=callback, schedulerEntry=entry)
	except Exception as error:
		entry.log(30, str(error))
		return False
	if started:
		entry.cancelFunction = lambda: cancelScheduled(entry)
	else:
		entry.log(30, _("A DVB-I task is already running."))
	return started


def cancelScheduled(entry=None):
	if entry is not None:
		for job in dvbiJobs():
			if job.schedulerEntry is entry:
				job.cancelRequested = True
				job.abort()


def registerScheduler():
	if not functionTimers.getItem(SCHEDULER_ID):
		# PythonTask handles the thread and delivers completion on the main loop.
		addFunctionTimer(SCHEDULER_ID, _("DVB-I channel update"), startScheduled, cancelScheduled, useOwnThread=True)


def newSchedule(clock=(3, 0)):
	local = list(localtime())
	local[3:6] = [int(clock[0]), int(clock[1]), 0]
	local[8] = -1
	begin = int(mktime(tuple(local)))
	if begin <= time():
		local[2] += 1
		begin = int(mktime(tuple(local)))
	entry = SchedulerEntry(begin, begin + 3600, timerType=TIMERTYPE.OTHER)
	entry.function = SCHEDULER_ID
	entry.repeated = 127
	return entry


class DvbIManagerSetup(Setup):
	"""Standard OpenATV setup.xml screen backed by persistent live choices."""

	def __init__(self, session):
		def screenClosed():
			self.uiActive = False

		self.uiActive = True
		self.updatingChoices = False
		refreshRegistryConfigChoices()
		Setup.__init__(self, session, setup="DvbIManager", plugin="Extensions/DvbIManager", PluginLanguageDomain=PLUGIN_DOMAIN)
		self.addSaveNotifier(activateHybrid)
		# ConfigListActions maps red/green on key-down; ColorActions uses key-up.
		# Replace only those save/cancel bindings, retaining normal config editing.
		self["fullUIActions"].setEnabled(False)
		self.setTitle(_("DVB-I channel search"))
		self["key_green"].setText(_("Create channel list"))
		self["key_yellow"] = StaticText(_("Update available lists"))
		self["key_blue"] = StaticText(_("Schedule updates"))
		self["key_info"] = StaticText(_("Last result"))
		self["dvbiActions"] = HelpableActionMap(
			self,
			["ColorActions", "OkCancelActions"],
			{
				"cancel": (self.keyCancel, _("Cancel any changed settings and exit")),
				"close": (self.closeRecursive, _("Cancel any changed settings and exit all menus")),
				"red": (self.keyRed, _("Save changed settings or close without changes")),
				"green": (self.createChannelList, _("Save settings and create the selected channel list")),
				"yellow": (self.refreshLiveCatalogue, _("Refresh the live DVB-I registry")),
				"blue": (self.openSchedule, _("Configure DVB-I updates in the Enigma2 scheduler")),
			},
			prio=0,
			description=_("DVB-I Manager Actions"),
		)
		self.actionMaps.append("dvbiActions")
		self["dvbiInfoActions"] = HelpableActionMap(
			self,
			"InfoActions",
			{
				"info": (self.showLastResult, _("Show the last DVB-I import result")),
			},
			prio=0,
		)
		SETUP_SCREENS.append(weakRef(self))
		self.onClose.append(screenClosed)
		self.onLayoutFinish.append(self.bootstrapLiveCatalogue)
		self.updateSaveButton()

	def updateSaveButton(self):
		self["key_red"].setText(_("Save") if self["config"].isChanged() else _("Cancel"))

	def createSetup(self, *args, **kwargs):
		Setup.createSetup(self, *args, **kwargs)
		self.updateSaveButton()

	def keyRed(self):
		if self["config"].isChanged():
			self.keySave()
		else:
			self.keyCancel()

	def openSchedule(self):
		from Screens.Timers import SchedulerEdit, SchedulerOverview

		try:
			selectedRegistryUrl()
		except ValueError as error:
			self.session.showWarning(str(error), timeout=6)
			return
		self.saveAll()
		scheduler = self.session.nav.Scheduler
		if any(entry.function == SCHEDULER_ID for entry in scheduler.timer_list):
			self.session.open(SchedulerOverview)
		else:
			self.session.openWithCallback(self.scheduleSaved, SchedulerEdit, newSchedule())

	def scheduleSaved(self, answer):
		if answer and answer[0]:
			self.session.nav.Scheduler.record(answer[1])
			self.session.showInfo(_("DVB-I update scheduled. Changes are available in the Enigma2 scheduler."), timeout=6)

	def showLastResult(self):
		try:
			manager = DvbIManager(dataDir=config.plugins.dvbi.cache_dir.value)
			data = manager.loadLastImport()
			if not data:
				self.session.showInfo(_("No previous DVB-I import result found."), timeout=5)
				return
			stats = data.get("stats", {})
			serviceList = data.get("service_list", {})
			bouquet = data.get("bouquet", {})
			text = (
				_(
					"Last DVB-I import\n"
					"List: {0}\n"
					"Source: {1}\n"
					"Services: {2}\n"
					"Written TV/radio: {3}/{4}\n"
					"Broadcast match/written: {5}/{6}\n"
					"IP written: {7}\n"
					"DRM/HbbTV marked: {8}/{9}\n"
					"EPG mapped/schedule sources: {10}/{11}\n"
					"Report: {12}\n"
					"Bouquets: TV {13}, Radio {14}"
				)
			).format(
				serviceList.get("name", ""),
				serviceList.get("source_url", ""),
				stats.get("services_total", 0),
				stats.get("services_written_tv", 0),
				stats.get("services_written_radio", 0),
				stats.get("services_matched_broadcast", 0),
				stats.get("services_written_broadcast", 0),
				stats.get("services_written_ip", 0),
				stats.get("services_drm_marked", 0),
				stats.get("services_hbbtv_marked", 0),
				data.get("epg", {}).get("stats", {}).get("services_with_content_guide", 0),
				data.get("epg", {}).get("stats", {}).get("services_with_schedule_endpoint", 0),
				data.get("report_path", ""),
				bouquet.get("bouquet_tv", ""),
				bouquet.get("bouquet_radio", ""),
			)
			self.session.open(TextBox, text, title=_("Last DVB-I import"))
		except Exception as err:
			self.session.showError(_("Could not read last DVB-I result: {0}").format(err), timeout=8)

	def bootstrapLiveCatalogue(self):
		state = registryCatalogState()
		refreshRegistryConfigChoices()
		self.createSetup()
		if state["state"] == "error":
			self.setFootnote(state["error"])
			self.session.showError(state["error"], timeout=8)
			return
		if state["state"] == "fresh":
			self.loadSelectedRegions()
			self.setFootnote(_("Choose a channel list and press green to add its free-to-air channels."))
			return
		self.setFootnote(
			_("Loading live registry data in the background...")
			if state["state"] == "missing"
			else _("Updating cached live registry data in the background...")
		)
		requestRegistryRefresh(self.session, force=False, notify=False)

	def loadSelectedRegions(self):
		if config.plugins.dvbi.registry_offering_id.value:
			try:
				request = buildRegionJob()
			except ValueError:
				request = None
			if request:
				launchTask(request, self.session, notify=False)

	def changedEntry(self):
		current = self.getCurrentItem()
		selectors = (
			config.plugins.dvbi.advanced,
			config.plugins.dvbi.registry_country,
			config.plugins.dvbi.registry_provider,
			config.plugins.dvbi.registry_language,
			config.plugins.dvbi.registry_delivery,
			config.plugins.dvbi.registry_offering_id,
		)
		if not self.updatingChoices and current in selectors:
			self.updatingChoices = True
			try:
				refreshRegistryConfigChoices(changed=current)
			finally:
				self.updatingChoices = False
		Setup.changedEntry(self)
		self.updateSaveButton()
		if current is config.plugins.dvbi.registry_offering_id and config.plugins.dvbi.registry_offering_id.value:
			try:
				regionJob = buildRegionJob()
			except Exception:
				regionJob = None
			if regionJob and regionJob.get("action") == "discover_regions":
				if launchTask(
					regionJob,
					self.session,
					notify=False,
				):
					self.setFootnote(_("Loading regions for the selected live service list..."))

	def createChannelList(self):
		if config.plugins.dvbi.enabled.value:
			state = registryCatalogState()
			if state["state"] == "missing":
				self.setFootnote(_("Live registry data is still loading. Select a service list when it is ready."))
				requestRegistryRefresh(self.session, force=False, notify=False)
				return
			try:
				selectedRegistryUrl()
			except Exception as error:
				self.session.showWarning(
					_("DVB-I settings are incomplete:\n{0}").format(error),
					timeout=8,
				)
				return

		# Let the standard Setup framework persist every setup.xml element.
		self.saveAll()
		activateHybrid()
		if config.plugins.dvbi.enabled.value:
			try:
				requestSelectedImport(self.session, force=True)
			except Exception as error:
				self.session.showError(
					_("Could not start DVB-I import:\n{0}").format(error),
					timeout=8,
				)
				return
		self.close()

	def refreshLiveCatalogue(self):
		if taskRunning():
			self.setFootnote(_("A DVB-I background task is already running."))
			return
		self.setFootnote(_("Refreshing live DVB-I registry data..."))
		if not requestRegistryRefresh(self.session, force=True, notify=True):
			self.session.showError(
				_("Could not start the live DVB-I registry refresh."),
				timeout=8,
			)

	def taskProgress(self, message):
		if self.uiActive:
			if config.plugins.dvbi.advanced.value:
				self.setFootnote(str(message or ""))
			else:
				self.setFootnote(_("Channel data is being checked in the background. You can continue watching TV."))

	def taskFinished(self, result):
		if not self.uiActive:
			return
		refreshRegistryConfigChoices()
		self.createSetup()
		self.setFootnote(formatTaskResult(result).replace("\n", "  "))
		if result.get("ok") and result.get("action") == "discover_registry":
			self.loadSelectedRegions()


def autostart(reason, session=None, **kwargs):
	"""Register the E2 task and hydrate cached live choices."""
	global SESSION
	if reason != 0:
		SESSION = None
		for job in dvbiJobs():
			job.cancelRequested = True
		activateHybrid(shutdown=True)
		if functionTimers.getItem(SCHEDULER_ID):
			functionTimers.remove(SCHEDULER_ID)
		return
	registerScheduler()
	if session is not None:
		SESSION = session
		activateHybrid()
		refreshRegistryConfigChoices()
		if config.plugins.dvbi.enabled.value and registryCatalogState()["state"] != "fresh":
			requestRegistryRefresh(session, force=False, notify=False)


def main(session, **kwargs):
	session.open(DvbIManagerSetup)


def menu(menuid, **kwargs):
	if menuid == "scan":
		return [(_("DVB-I channel search"), main, "dvbi_manager", 60)]
	return []


def pluginIcon(width=None):
	if width is None:
		width = getDesktop(0).size().width()
	return "pluginfhd.png" if width >= 1920 else "plugin.png"


def Plugins(**kwargs):
	registerScheduler()
	descriptors = [
		PluginDescriptor(
			name=_("DVB-I Manager"),
			description=_("Create TV and radio channel lists from DVB-I"),
			where=PluginDescriptor.WHERE_PLUGINMENU,
			icon=pluginIcon(),
			fnc=main,
		),
		PluginDescriptor(
			name=_("DVB-I channel search"),
			description=_("Create TV and radio channel lists from DVB-I"),
			where=PluginDescriptor.WHERE_MENU,
			fnc=menu,
		),
		PluginDescriptor(
			where=PluginDescriptor.WHERE_SESSIONSTART,
			fnc=autostart,
		),
	]
	# WHERE_AUTOSTART also receives removal/shutdown, unlike SESSIONSTART.
	descriptors.append(PluginDescriptor(where=PluginDescriptor.WHERE_AUTOSTART, fnc=autostart))
	descriptors.append(
		PluginDescriptor(
			name=_("DVB-I Resolver"),
			description=_("Resolve stable DVB-I playback targets"),
			where=PluginDescriptor.WHERE_PLAYSERVICE,
			needsRestart=False,
			fnc=resolveDvbiService,
		)
	)
	return descriptors
