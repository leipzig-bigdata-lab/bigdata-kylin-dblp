package de.bigprak.transform.schema;

public class Publication {
	private String publicationId;
	private int titleId;
	private int documentTypeId;
	private int timeId;
	private int venueSeriesId;
	private int citingsDblp;
	private int citingsGs;
	private int citingsAcm;
	private int citingsAcmSelf;
	
	public String getPublicationId() {
		return publicationId;
	}
	public void setPublicationId(String publicationId) {
		this.publicationId = publicationId;
	}
	public int getTitleId() {
		return titleId;
	}
	public void setTitleId(int titleId) {
		this.titleId = titleId;
	}
	public int getDocumentTypeId() {
		return documentTypeId;
	}
	public void setDocumentTypeId(int documentTypeId) {
		this.documentTypeId = documentTypeId;
	}
	public int getTimeId() {
		return timeId;
	}
	public void setTimeId(int timeId) {
		this.timeId = timeId;
	}
	public int getVenueSeriesId() {
		return venueSeriesId;
	}
	public void setVenueSeriesId(int venueSeriesId) {
		this.venueSeriesId = venueSeriesId;
	}
	public int getCitingsDblp() {
		return citingsDblp;
	}
	public void setCitingsDblp(int citingsDblp) {
		this.citingsDblp = citingsDblp;
	}
	public int getCitingsGs() {
		return citingsGs;
	}
	public void setCitingsGs(int citingsGs) {
		this.citingsGs = citingsGs;
	}
	public int getCitingsAcm() {
		return citingsAcm;
	}
	public void setCitingsAcm(int citingsAcm) {
		this.citingsAcm = citingsAcm;
	}
	public int getCitingsAcmSelf() {
		return citingsAcmSelf;
	}
	public void setCitingsAcmSelf(int citingsAcmSelf) {
		this.citingsAcmSelf = citingsAcmSelf;
	}
}
