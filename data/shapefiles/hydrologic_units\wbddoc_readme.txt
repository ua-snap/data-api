The USDA Geospatial Data Gateway version of the Watershed Boundary Dataset (WBD) layers are only updated twice per year 
from the USGS.  Note: see the “LoadDate” field in the attribute tables for the vintage of the WBD data from Gateway.
If a more recent copy is required for your particular project, the USGS NHD website hosts data that is 
updated at least once every quarter either via the National Map or pre-staged zipped files in various ways.  
The USGS WBD data can be downloaded from the following link: http://nhd.usgs.gov/data.html. 

****************************************************************************************************************************
The document "Federal Standards and Procedures for the National Watershed Boundary Dataset (WBD)" can be found at:
http://pubs.usgs.gov/tm/tm11a3/pdf/WBD-Ed3_052212.pdf

****************************************************************************************************************************
In addition to the Gateway application's WBD delivery mechanisms by County, State, or other Area of Interest, National seamless
layers of the WBD by Hydrologic Unit (HU) digit level may also be downloaded from the following USDA NRCS download site:
https://nrcs.app.box.com/v/huc.

****************************************************************************************************************************

The hydrologic unit data that you have downloaded from the USDA Geospatial Data Gateway is called the Watershed Boundary
Dataset (WBD). This dataset at 1:24,000 scale is a greatly expanded version of the hydrologic units created in the 
mid-1970's by the U.S. Geological Survey under the sponsorship of the Water Resources Council. The WBD is a complete set of
hydrologic units from new watershed and subwatesheds less than 10,000 acres to entire river systems draining large hydrologic
unit regions, all attributed by a standard nomenclature.

Development of the Watershed Boundary Dataset started in the early 1990's and has progressed to the format and attribution
that is now being distributed.  The delineation and attribution was done on a state basis using a variety of methods and 
source data.  Each state HU dataset has gone through an extensive quality review process to ensure accuracy and compliance
to the Federal Standard for Delineation of Hydrologic Unit Boundaries 
(http://www.nrcs.usda.gov/wps/portal/nrcs/detail/national/water/watersheds/?cid=nrcs143_021630)
before and during submittal to USDA-NRCS National Geospatial Center of Excellence (NGCE).  

NRCS modified the WBD dataset with the intention of creating a seamless dataset for the entire country by all 6 levels.
The data is delivered by sub-basin and includes data for HUC-8, HUC-10, and HUC-12.  The deliverable includes attributes for
hydrologic unit codes, hydrologic unit name, downstream hydrologic unit, man-made modifications to overland flow that alter
the location of the HU boundary, and HU type for each hydrologic unit level 1-6.  An acres field already exists for each 
subwatershed.  The polygon ESRI shapefile has an accompanying line shapefile of the same boundaries with attribute fields 
for HU level, linesource, and metadata ID.  At this time, not all records have attributes for all the fields, but may be 
filled in by the user for local projects or will be filled in at a later date.  For a complete description of the attributes
and processes used to delineate hydrologic units to 1:24,000 scale accuracy, please refer to accompanying document titled
Watershed Boundary Dataset (WBD) User Guide (wbddoc_user_guide.doc).  It should be noted that if a HUC code field 
(ex. HUC_12) has "00" for the last two digits, this means that is has the same boundary as the level above it.  This means
the level above the "00" HU was not subdivided any smaller.  This may have occurred if the base data does not have enough
detail to acccuractly delineate the boundaries with confidence.  When this happens the HUC has "00" added on the existing
code at the level above it and the boundaries remain the same.  At a later date this HUC may be delineated when better 
base data becomes available.   

A generalized metadata file was created by the Geospatial Data Gateway for each downloaded dataset.  Another accompanying
document titled wbd_state_metadata.html includes links for state specific HU metadata. The state metadata provides information
on how each state delineated their hydrologic units.

The definition for a hydrologic unit according to the FGDC Proposal, Version 1.0 - Federal Standards For Delineation of 
Hydrologic Unit Boundaries 3/01/02 states “A hydrologic unit is a drainage area delineated to nest in a multi-level, 
hierarchical drainage system.  Its boundaries are defined by hydrographic and topographic criteria that delineate an 
area of land upstream from a specific point on a river, stream or similar surface waters.  A hydrologic unit can accept 
surface water directly from upstream drainage areas, and indirectly from associated surface areas such as remnant, 
non-contributing, and diversions to form a drainage area with single or multiple outlet points. Hydrologic units are 
only synonymous with classic watersheds when their boundaries include all the source area contributing surface water to a 
single defined outlet point. “  

The EPA hosts a website, http://cfpub.epa.gov/surf/locate/index.cfm, that supplies local citizen based groups that are 
active in a subbasin (hu8 code) for your particular area of interest. 

****************************************************************************************************************************
Currently, harmonization with Canada and Mexico is underway.  Some hydrologic units at the sub-basin level may exist only at
the 8-digit level in Canada (missing 10- and 12-digit subdivisions), but this is only temporary until harmonization is 
completed.