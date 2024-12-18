import json 
from datetime import datetime, timedelta
import re 
import requests

# Hardcoded variables for the STAC to GeoCore translation 
status = 'active'
maintenance = 'active' 
useLimits_en = 'RADARSAT Constellation Mission (RCM) - Public User License Agreement https://www.asc-csa.gc.ca/eng/satellites/radarsat/access-to-data/public-user-license-agreement.asp'
useLimits_fr = 'Mission de la Constellation RADARSAT (MCR) - Contrat de licence d\'utilisateur public https://www.asc-csa.gc.ca/fra/satellites/radarsat/acces-aux-donnees/contrat-licence-utilisateur-public.asp'
spatialRepresentation = 'grid; grille'
type_data = "Synthetic Aperature Radar; Radar à synthèse d'ouverture"
topicCategory = 'EarthObservation;SyntheticAperatureRadar'
disclaimer_en = '\\n\\n**This third party metadata element follows the Spatio Temporal Asset Catalog (STAC) specification.**'
#disclaimer_fr = '\\n\\n**Cet élément de métadonnées tiers suit la spécification Spatio Temporal Asset Catalog (STAC).** **Cet élément de métadonnées provenant d’une tierce partie a été traduit à l\'aide d\'un outil de traduction automatisée (Amazon Translate).**'
disclaimer_fr = '\\n\\n**Cet élément de métadonnées tiers suit la spécification Spatio Temporal Asset Catalog (STAC).**'
coll_description_en = "The RADARSAT Constellation Mission (RCM) is Canada's third generation of Earth observation satellites. Launched on June 12, 2019, the three identical satellites work together to bring solutions to key challenges for Canadians. As part of ongoing Open Government efforts, NRCan has developed a CEOS analysis ready data (ARD) processing capability for RCM and is processing the Canada-wide, 30M Compact-Polarization standard coverage, every 12 days. Previously, users were stuck ordering, downloading and processing RCM images (level 1) on their own, often with expensive software. This new dataset aims to remove these burdens with a new STAC catalog for discover and direct download from S3."
coll_description_fr = "La mission de la Constellation RADARSAT (MCR) est la troisième génération de satellites d'observation de la Terre du Canada. Lancés le 12 juin 2019, les trois satellites identiques travaillent ensemble pour apporter des solutions aux principaux défis des Canadiens. Dans le cadre des efforts continus pour un gouvernement ouvert, RNCan a développé une capacité de traitement des données prêtes à l'analyse (DPA) du CEOS pour le MCR et traite la couverture standard de polarisation compacte de 30 M à l'échelle du Canada, tous les 12 jours. Auparavant, les utilisateurs étaient obligés de commander, de télécharger et de traiter eux-mêmes les images RCM (niveau 1), souvent à l'aide de logiciels coûteux. Ce nouvel ensemble de données vise à supprimer ces fardeaux avec un nouveau catalogue STAC à découvrir et à télécharger directement depuis S3."
coll_keywords_fr = "MCR, radar, observation de la Terre, ESA, La mission de la Constellation RADARSAT" 

contact = [{
        'organisation':{
            'en':'Government of Canada;Natural Resources Canada;Strategic Policy and Innovation Sector',
            'fr':'Gouvernement du Canada;Ressources naturelles Canada;Secteur de la politique stratégique et de l’innovation'
            }, 
            'email':{
                'en':'eodms-sgdot@nrcan-rncan.gc.ca',
                'fr':'eodms-sgdot@nrcan-rncan.gc.ca'
            }, 
            'individual': None, 
            'position': {
                'en': None,
                'fr': None
                },
            'telephone':{
            'en': None,
            'fr': None
            },
            'address':{
            'en': '580 Booth St',
            'fr': '580 Booth St'
            },
            'city':'Ottawa',
            'pt':{
                'en': 'Ontario',
                'fr': 'Ontario'
                },
            'postalcode': 'K1A 0E4', 
            'country':{
                'en': 'Canada', 
                'fr': 'Canada'
                },
            'onlineResources':{
                'onlineResources': None,
                'onlineResources_Name': None,
                'onlineResources_Protocol': None,
                'onlineResources_Description': None 
                },
            'hoursofService': None, 
            'role': None, 
        }]
        
# STAC to GeoCore translation functions 
def update_dict(target_dict, updates):
    """Utility function to update a dictionary with new key-value pairs.

    Parameters:
    - target_dict: The original dictionary to update.
    - updates: A dictionary containing the updates.

    Returns:
    - The updated dictionary.
    """
    target_dict.update(updates)
    return target_dict

def update_geocore_dict(geocore_features_dict, properties_dict, geometry_dict):
    """Update the GeoCore geocore_features_dict null template with the updated properties and geometry dictionaries.
    
    Parameters:
    - geocore_features_dict: The initial GeoCore dictionary.
    - properties_dict: The updated properties dictionary.
    - geometry_dict: The updated geometry dictionary.
    
    Returns:
    - A new GeoCore dictionary with updated features.
    """
    if not isinstance(properties_dict, dict) or not isinstance(geometry_dict, dict):
        raise ValueError("properties_dict and geometry_dict must be dictionaries.")
    
    updated_dict = geocore_features_dict.copy()
    updated_dict = update_dict(updated_dict, {"properties": properties_dict, "geometry": geometry_dict})
    return {
        "type": "FeatureCollection",
        "features": [updated_dict]
    }

#stac_to_feature_geometry
def to_features_geometry(geocore_features_dict, bbox, geometry_type='Polygon'):
    """Mapping to GeoCore features geometry field.
    
    :param bbox: list of bounding box [west, south, east, north]
    :param geometry_type: string of item or collection type, default is 'Polygon'
    """
    geometry_dict = geocore_features_dict['geometry']
    west, south, east, north = [round(coord, 2) for coord in bbox]
    coordinates=[[[west, south], [east, south], [east, north], [west, north], [west, south]]]
    # Update the geometry dictionary
    updates = {
        "type": geometry_type,
        "coordinates": coordinates
    }
    update_dict(geometry_dict, updates)
    
    return geometry_dict

# A function to map STAC links to GeoCore option 
def links_to_properties_options(links_list, id, root_name, title_en, title_fr, stac_type): 
    """Mapping STAC Links object to GeoCore features properties options  
    :param links_list: STAC collection or item links object
    :param id: collection id or item id 
    :param api_name_en/api_name_fr: STAC datacube English/French nama, hardcoded variables 
    :param coll_title: 
    :param stac_type: item or collection 
    """   
    return_list = []
    root_name_en,root_name_fr = root_name.split('/')
    for var in links_list: 
        href, rel, type_str, name = var.get('href'), var.get('rel'), var.get('type', '').replace(';', ','), var.get('title')
        name_en, name_fr = {
            'collection': (None, None),
            'derived_from': (None, None),
            'self': ('Self - ' + id if stac_type != 'root' else 'Root - ' + root_name_en, 'Soi - ' + id if stac_type != 'root' else 'Racine - ' + root_name_fr),
            'root': ('Root - ' + root_name_en, 'Racine - ' + root_name_fr),
            'data': ('Collection - ' + root_name_en, 'Collecte - ' + root_name_fr),
            'parent': ('Parent - ' + title_en if stac_type == 'item' and title_en else 'Parent links', 'Parente - ' + title_fr if stac_type == 'item' and title_fr else 'Parente liens'),
            'items': ('Items API', 'Éléments la API')
        }.get(rel, (name if name else 'Unknown', name if name else 'Inconnue'))
                
        if name_en and name_fr:
            option_dic = {
                "url": href,
                "protocol": 'Unknown',
                "name": {"en": name_en, "fr": name_fr},
                "description": {"en": f'unknown;{type_str};eng', "fr": f'unknown;{type_str};fra'}
            }        
            return_list.append(option_dic)
    return (return_list)

# A function to map STAC assets to GeoCore option 
def assets_to_properties_options(assets_list): 
    """Mapping STAC Links object to GeoCore features properties options  
    :param assets_list: STAC collection or item assets object
    :return return list: geocore features properties option list  
    """ 
    return_list = []
    for var_dict in assets_list.values():
        href, type_str, name = var_dict.get('href'), var_dict.get('type', '').replace(';', ','), var_dict.get('title', 'Unknown/Inconnu')
        name_en, name_fr = name.split('/') if '/' in name else (name, name)
        option_dic = {
            "url": href,
            "protocol": 'Unknown',
            "name": {"en": f'Asset - {name_en}', "fr": f'Asset - {name_fr}'},
            "description": {"en": f'unknown;{type_str};eng', "fr": f'unknown;{type_str};fra'}
        }
        return_list.append(option_dic)
    return return_list

#root_to_features_properties 
def root_to_features_properties(params, geocore_features_dict): 
    # Get the parameters 
    root_name = params['root_name']
    root_links = params['root_links']
    root_id = params['root_id']
    source = params['source']
    root_des = params['root_des'] 
    root_bbox = params['root_bbox'] 
    status = params['status']
    maintenance = params['maintenance'] 
    useLimits_en = params['useLimits_en']
    useLimits_fr = params['useLimits_fr']
    spatialRepresentation = params['spatialRepresentation']
    contact = params['contact']
    type_data = params['type_data']
    topicCategory = params['topicCategory']
    sourceSystemName = params['sourceSystemName']
    
    
    properties_dict = geocore_features_dict['properties']
    root_name_en,root_name_fr = root_name.split('/')
    #id
    update_dict(properties_dict, {"id": f"{root_id}-root"})
    #title 
    update_dict(properties_dict['title'], {"en": f" Root  - {root_name_en}"})
    update_dict(properties_dict['title'], {"fr": f" Racine - {root_name_fr}"})    

    #options  
    links_list = links_to_properties_options(links_list=root_links, id=root_id, root_name=root_name, title_en=None, title_fr=None, stac_type='root')
    options_list = links_list
    #print(f'This is option list before delete duplication: {json.dumps(options_list, indent=2)}')
    options_list = [i for n, i in enumerate(options_list) if i not in options_list[n + 1:]] # delete duplicates
    #print(f'This is option list after delete duplication: {json.dumps(options_list, indent=2)}')
    
    #Descrption 
    en_desc = root_des + '.' + disclaimer_en if root_des else disclaimer_en
    fr_desc = root_des + '.' + disclaimer_fr if root_des else disclaimer_fr
    update_dict(properties_dict['description'], {'en': en_desc, 'fr': fr_desc})
    
    #Keywords 
    keywords_common = 'SpatioTemporal Asset Catalog, stac'
    update_dict(properties_dict['keywords'], {'en': f"{keywords_common}, {source}", 'fr': f"{keywords_common}, {source}"})
 
    #Geometry 
    west, south, east, north = [round(coord, 2) for coord in root_bbox]
    geometry_str = f"POLYGON(({west} {south}, {east} {south}, {east} {north}, {west} {north}, {west} {south}))"
    update_dict(properties_dict, {"geometry": geometry_str})
    
    # Other properties
    update_dict(properties_dict, {
        'topicCategory': topicCategory,
        'type': type_data,
        'spatialRepresentation': spatialRepresentation,
        'status': status,
        'maintenance': maintenance,
        'contact': contact,
        'options': options_list,
        'useLimits': {'en': useLimits_en, 'fr': useLimits_fr},
        'temporalExtent': {'end': 'Present', 'begin': '0001-01-01'},
        'sourceSystemName': sourceSystemName, 
    })
    #parentIdentifier: None for STAC catalog   
    #date: None for STAC collection 
    #skipped: refsys, refSys_version  
    #skipped metadataStandard, metadataStandardVersion, metadataStandardVersion, graphicOverview, distributionFormat_name, distributionFormat_format
    #skipped: accessConstraints, otherConstraints, dateStamp, dataSetURI, locale,language
    #skipped: characterSet, environmentDescription,supplementalInformation
    #skipped: credits, cited, distributor,sourceSystemName
    return (properties_dict)

#collection_to_features_properties 
def coll_to_features_properties(params, coll_dict,geocore_features_dict): 
    # Get the parameters 
    root_name = params['root_name']
    root_id = params['root_id']
    source = params['source']
    status = params['status']
    maintenance = params['maintenance'] 
    useLimits_en = params['useLimits_en']
    useLimits_fr = params['useLimits_fr']
    spatialRepresentation = params['spatialRepresentation']
    contact = params['contact']
    type_data = params['type_data']
    topicCategory = params['topicCategory']
    sourceSystemName = params['sourceSystemName']
    eoCollection = params['eoCollection']

    properties_dict = geocore_features_dict['properties']
    
    coll_id, coll_bbox, time_begin, time_end, coll_links, coll_assets, title_en, title_fr, description_en, description_fr, keywords_en, keywords_fr = get_collection_fields(coll_dict, coll_description_en,coll_description_fr,coll_keywords_fr)     
    #id
    update_dict(properties_dict, {"id": source + '-' + coll_id})
    #title 
    if title_en != None and title_fr!= None: 
        update_dict(properties_dict, {'title':{'en':'Collection - ' + title_en, 'fr':'Collection - ' + title_fr}})
        
    #parentIdentifier: root id 
    update_dict(properties_dict, {"parentIdentifier":  root_id + '-root'})
    #temporalExtent
    time_begin_str = format_datetime_for_json(time_begin) if time_begin else '0001-01-01'
    time_end_str = format_datetime_for_json(time_end) if time_end else 'Present'
    temporal_extent_updates = {"begin": time_begin_str, "end": time_end_str}
    update_dict(properties_dict['temporalExtent'], temporal_extent_updates)

    #options  
    links_list = links_to_properties_options(links_list=coll_links, id=coll_id, root_name=root_name, title_en=title_en, title_fr=title_fr, stac_type='collection')
    assets_list = assets_to_properties_options(assets_list=coll_assets) if coll_assets else []
    options_list = links_list+assets_list
    options_list = [i for n, i in enumerate(options_list) if i not in options_list[n + 1:]] # delete duplicates


    # The shared attributes between Items and Collections  
    description_en_str = f"{description_en or ''} {disclaimer_en}"
    description_fr_str = f"{description_fr or ''} {disclaimer_fr}"
    keywords_en_str = f"SpatioTemporal Asset Catalog, stac, {keywords_en or ''}"
    keywords_fr_str = f"SpatioTemporal Asset Catalog, stac, {keywords_fr or ''}"
    
    #Geometry 
    #print("Value of coll_bbox:", coll_bbox)
    #print("Type of coll_bbox:", type(coll_bbox))
    #for val in coll_bbox:
    #    print(val, type(val))
    west, south, east, north = [round(coord, 2) for coord in coll_bbox]
    geometry_str = f"POLYGON(({west} {south}, {east} {south}, {east} {north}, {west} {north}, {west} {south}))"
    
    # Other properties 
    update_dict(properties_dict, {
        "topicCategory": topicCategory, 
        "type": type_data, 
        "spatialRepresentation":spatialRepresentation,
        "status":status,
        "maintenance":maintenance,
        'useLimits': {'en': useLimits_en, 'fr': useLimits_fr},
        'contact': contact,
        'options': options_list, 
        'description': {'en': description_en_str, 'fr': description_fr_str},
        'keywords': {'en': keywords_en_str, 'fr': keywords_fr_str},
        "geometry": geometry_str, 
        'sourceSystemName': sourceSystemName, 
        'eoCollection':eoCollection,
          
    })
     
    #skipped: date: None for STAC collection 
    #skipped: refsys, refSys_version  
    #skipped metadataStandard, metadataStandardVersion, metadataStandardVersion, graphicOverview, distributionFormat_name, distributionFormat_format
    #skipped: accessConstraints, otherConstraints, dateStamp, dataSetURI, locale,language
    #skipped: characterSet, environmentDescription,supplementalInformation
    #skipped: credits, cited, distributor,sourceSystemName
    # options 
    return (properties_dict)

#TODO Update these get_collection_fields functions for fr keywords, description, and coll_title
def get_collection_fields(coll_dict,coll_description_en,coll_description_fr,coll_keywords_fr): 
    """Get the collection fields needed for the geocore mapping 
    :param coll_dict: dictionary of a singel STAC collection 
    """
    # Directly extract values using .get() method
    #.get() method allows you to provide a default value (in this case, None) if the key is not found.
    coll_id = coll_dict.get('id')
    coll_title = coll_dict.get('title')
    #coll_description = coll_dict.get('description')
    coll_keywords = coll_dict.get('keywords')
    coll_extent = coll_dict.get('extent')
    coll_links = coll_dict.get('links')
    coll_assets = coll_dict.get('assets')
        
    # Get bbox and time 
    coll_bbox, time_begin, time_end = None, None, None
    if coll_extent:
        coll_bbox = coll_extent.get('spatial', {}).get('bbox', [None])
        temporal_interval = coll_extent.get('temporal', {}).get('interval', [[None, None]])[0]
        time_begin, time_end = temporal_interval  
    
    # Get English and French for description, keywords, and title
    """
    title_en, title_fr = (coll_title.split('/') + [coll_id, coll_id])[:2] if coll_title else (coll_id, coll_id)
    description_en, description_fr = (coll_description.split('/') + [None, None])[:2] if coll_description else (None, None)
    if coll_keywords:
        half_length = len(coll_keywords) // 2
        keywords_en = ', '.join(str(l) for l in coll_keywords[:half_length])
        keywords_fr = ', '.join(str(l) for l in coll_keywords[half_length:])
    else:
        keywords_en, keywords_fr = None, None
    """    
    #Note, EODMS STAC API endpoint is not bilingual content at the moment, we will use staic Fr translations for these properties 
    title_en, title_fr = coll_title, coll_title
    description_en, description_fr = coll_description_en, coll_description_fr
    keywords_en, keywords_fr = ', '.join(coll_keywords), coll_keywords_fr

    return coll_id, coll_bbox, time_begin, time_end, coll_links, coll_assets, title_en, title_fr, description_en, description_fr, keywords_en, keywords_fr

def create_coll_dict(api_root, collection, coll_title_fr,coll_description_fr,coll_keywords_fr):
    response_collection = requests.get(f'{api_root}/collections/')
    collection_data_list = response_collection.json().get('collections', [])
    #subset to one collection based on id
    collection_data_list = [item for item in collection_data_list if item.get('id') == collection]

    coll_id_dict = {
        coll_dict['id']: {
            "title": {'en': fields[6], 'fr': fields[7]},
            'description': {'en': fields[8], 'fr': fields[9]},
            'keywords': {'en': fields[10], 'fr': fields[11]},
        }
        for coll_dict in collection_data_list
        for fields in [get_collection_fields(coll_dict,coll_description_en,coll_description_fr,coll_keywords_fr)]
    }
    return coll_id_dict 


#Item_to_features_properties
def item_to_features_properties(params, geocore_features_dict, item_dict, coll_id_dict):
    root_name = params['root_name']
    root_id = params['root_id']
    source = params['source']
    status = params['status']
    maintenance = params['maintenance'] 
    useLimits_en = params['useLimits_en']
    useLimits_fr = params['useLimits_fr']
    spatialRepresentation = params['spatialRepresentation']
    contact = params['contact']
    type_data = params['type_data']
    topicCategory = params['topicCategory']
    sourceSystemName = params['sourceSystemName']
    eoCollection = params['eoCollection']
    
    properties_dict = geocore_features_dict['properties']
    # Get item level lelments 
    item_id, item_bbox, item_links, item_assets, item_properties,coll_id = get_item_fields(item_dict) 
    
    # Get collection level keywords, title, and description 
    coll_data = coll_id_dict.get(coll_id, {})
    title_en = coll_data.get('title', {}).get('en')
    title_fr = coll_data.get('title', {}).get('fr')
    description_en = coll_data.get('description', {}).get('en')
    description_fr = coll_data.get('description', {}).get('fr')
    keywords_en = coll_data.get('keywords', {}).get('en')
    keywords_fr = coll_data.get('keywords', {}).get('fr')
    
    #id
    properties_dict.update({"id": source + '-' + coll_id + '-' + item_id})
     
    #date
    default_date = datetime(1900, 1, 1)  # Replace with a sensible default
    try:
        item_start_date = datetime.strptime(item_properties['datetime'], '%Y-%m-%dT%H:%M:%S.%f%z')
    except ValueError:
        # Handle the exception here
        print("The date format of 'item_properties['datetime']' does not match the expected format.")
        # You might want to set item_start_date to None or a default value, or re-raise the exception, depending on your use case
        item_start_date = default_date
    
    try:
        item_end_date = datetime.strptime(item_properties['end_datetime'], '%Y-%m-%dT%H:%M:%S.%f%z')
    except ValueError:
        # Handle the exception here
        print("The date format of 'item_properties['end_datetime']' does not match the expected format.")
        # You might want to set item_end_date to None or a default value, or re-raise the exception, depending on your use case
        item_end_date = 'Present'

    #TODO update the item level title
    #title
    item_title = item_properties['title'].replace("_", "-")
    if title_en != None and title_fr!= None: 
        update_dict(properties_dict, {'title':{'en':'Record - ' + item_title + '-' + title_en, 'fr':'Ficher - ' + item_title + '-' + title_fr}})
    #parentIdentifier
    update_dict(properties_dict, {"parentIdentifier":  source + '-'+ coll_id})
    
    #TemporalExtent 
    if 'created' in item_properties.keys(): 
        item_created = item_properties['created']
        update_dict(properties_dict['date']['published'], {
        "text": 'publication; publication',
        "date": format_datetime_for_json(item_created)
        })
        
        update_dict(properties_dict['date']['created'], {
        "text": 'creation; création',
        "date": format_datetime_for_json(item_created)
        })
    #temporalExtent: begin is the datatime, hard coded 'Present'as end   
    update_dict(properties_dict['temporalExtent'], {
    "begin": format_datetime_for_json(item_start_date),
    "end": format_datetime_for_json(item_end_date)})
    
    #options  
    links_list = links_to_properties_options(links_list=item_links, id=item_id, root_name=root_name, title_en=title_en, title_fr=title_fr, stac_type='item')
    assets_list = assets_to_properties_options(assets_list=item_assets) if item_assets else []
    options_list = links_list+assets_list
    options_list = [i for n, i in enumerate(options_list) if i not in options_list[n + 1:]] # delete duplicates
        
    # The shared attributes between Items and Collections  
    description_en_str = f"{description_en or ''} {disclaimer_en}"
    description_fr_str = f"{description_fr or ''} {disclaimer_fr}"
    keywords_en_str = f"SpatioTemporal Asset Catalog, stac, {keywords_en or ''}"
    keywords_fr_str = f"SpatioTemporal Asset Catalog, stac, {keywords_fr or ''}"

    #Geometry 
    west, south, east, north = [round(coord, 2) for coord in item_bbox]
    geometry_str = f"POLYGON(({west} {south}, {east} {south}, {east} {north}, {west} {north}, {west} {south}))"
    
    #EO filters / SAR properties 
    orbit_state = item_properties.get('sat:orbit_state', 'None')
    polarizations = item_properties.get('sar:polarizations', 'None') #list
    polarizations_str = polarization_to_string(polarizations)
    eoFilters = [ 
        {
			"polarizations":polarizations_str,
			"orbitState": orbit_state
		}
    ]
    
    # Other properties 
    update_dict(properties_dict, {
        "topicCategory": topicCategory, 
        "type": type_data, 
        "spatialRepresentation":spatialRepresentation,
        "status":status,
        "maintenance":maintenance,
        'useLimits': {'en': useLimits_en, 'fr': useLimits_fr},
        'contact': contact,
        'options': options_list, 
        'description': {'en': description_en_str, 'fr': description_fr_str},
        'keywords': {'en': keywords_en_str, 'fr': keywords_fr_str},
        "geometry": geometry_str, 
        'sourceSystemName': sourceSystemName, 
        'eoCollection':eoCollection,
        'eoFilters':eoFilters,   
    })
     
    #skipped: date: None for STAC collection 
    #skipped: refsys, refSys_version  
    #skipped metadataStandard, metadataStandardVersion, metadataStandardVersion, graphicOverview, distributionFormat_name, distributionFormat_format
    #skipped: accessConstraints, otherConstraints, dateStamp, dataSetURI, locale,language
    #skipped: characterSet, environmentDescription,supplementalInformation
    #skipped: credits, cited, distributor,sourceSystemName
    # options 
    return (properties_dict)


def get_item_fields(item_dict): 
    """Get the collection fields needed for the geocore mapping 
    :param item_dict: dictionary of a singel STAC item  
    """
    item_id = item_dict.get('id')
    item_bbox = item_dict.get('bbox')
    item_links = item_dict.get('links')
    item_assets = item_dict.get('assets')
    item_properties = item_dict.get('properties')
    coll_id = item_dict.get('collection')
    return item_id, item_bbox, item_links, item_assets, item_properties, coll_id; 

def polarization_to_string(polarization):
    # Check if the polarization is None
    if polarization is None:
        return 'None'

    # Check if the length of the polarization list is 2
    if len(polarization) == 2:
        # Join the two polarizations with a '+'
        return ' + '.join(polarization)
    elif len(polarization) == 1:
        # Return the single polarization as a string
        return polarization[0]
    else:
        # Handle other cases, such as an empty list or more than 2 items
        return 'Invalid polarization list'

def format_datetime_for_json(date_input, default_date=datetime(1900, 1, 1)):
    """
    Formats a datetime object or a datetime string in ISO format to the desired format.
    If parsing fails, returns a default date formatted in the same way.
    """
    # If date_input is a string, parse it; if it's already a datetime, use it directly
    if isinstance(date_input, str):
        try:
            date_obj = datetime.strptime(date_input, '%Y-%m-%dT%H:%M:%S.%f%z')
        except ValueError:
            try:
                date_obj = datetime.strptime(date_input, '%Y-%m-%d %H:%M:%S.%f%z')
            except ValueError:
                print("The date format does not match the expected format. Using default date.")
                date_obj = default_date
    elif isinstance(date_input, datetime):
        date_obj = date_input
    else:
        raise TypeError("date_input must be a string or a datetime object")

    # Format with "Z" for UTC or with timezone offset for others
    if date_obj.utcoffset() == timedelta(0):
        formatted_date = date_obj.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    else:
        formatted_date = date_obj.strftime("%Y-%m-%dT%H:%M:%S.%f%z")

    return formatted_date
