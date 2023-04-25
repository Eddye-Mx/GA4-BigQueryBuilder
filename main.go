package main

import (
	"fmt"
	"strings"
)

var entriesToOutputs = map[string]string{
	"campaign": "traffic_source.name as campaign", /*Dimension*/
	"source": "traffic_source.source as source",/*Dimension*/
	"medium": "traffic_source.medium as medium",/*Dimension*/
	"date": "event_date as date",/*Dimension*/
	"sessions": "COUNT(case event_name when 'session_start' then 1 else null end) as sessions",/*Metric*/
	"pageviews": "COUNT(case event_name when 'page_view' then 1 else null end) as pageviews",/*Metric*/
	"users": "COUNT(DISTINCT user_pseudo_id) as users",/*Metric*/
	"new_users": "COUNT(case event_name when 'first_visit' then 1 else null end) as new_users",/*Metric*/
	"revenue": "SUM(ecommerce.purchase_revenue) as revenue",/*Metric*/
	"transactions": "COUNT(case event_name when 'purchase' then 1 else null end) as transactions",/*Metric*/
	"item_revenue": "SUM(i.item_revenue) as item_revenue",/*Metric*/
}

var clientToDatasets = map[string]accountInfo{
	"innovasport": {
		Project: "innovasport",
		Dataset: "analytics_274438246",
	},
}

var payload = Payload{
	Accounts: []AccountData{
	   {
		  Name:"innovasport",
		  View_id:"202729775",
	   },
	},
	Dimensions:[]string{
		"date",
	   "source",
	   "medium",
	   "campaign",
	},
	Metrics:[]string{
	   "users",
	   "new_users",
	   "pageviews",
	   "transactions",
	   "revenue",
	},
	Start_date:"2023-04-10",
	End_date:"2023-04-12",
}

func main() {
	fmt.Println(concatSQL(payload))

}


func concatSQL(payload Payload) (string, error){
	toReturn := "SELECT "
	toReturn += entriesToOutputs[payload.Dimensions[0]] 
	if(len(payload.Dimensions)>0){
		toReturn += ","
		for i:= 1; i<len(payload.Dimensions); i++ {
			toReturn += entriesToOutputs[payload.Dimensions[i]] 
			toReturn += ","
		}
	}
	toReturn += entriesToOutputs[payload.Metrics[0]] 
	if(len(payload.Metrics)>0){
		for i:= 1; i<len(payload.Metrics); i++ {
			toReturn += ","
			toReturn += entriesToOutputs[payload.Metrics[i]] 
		}
	}
	groupBy := "group by " + payload.Dimensions[0]
	if(len(payload.Dimensions)>0){
		for i:= 1; i<len(payload.Dimensions); i++ {
			groupBy += ","
			groupBy += payload.Dimensions[i]
		}
	}
	orderBy := fmt.Sprintf("order by %v",(len(payload.Dimensions)+1))
	fmt.Println(groupBy)
	sDateParsed := strings.ReplaceAll(payload.Start_date, "-", "")
	eDateParsed := strings.ReplaceAll(payload.End_date, "-", "")
	ProjectInfo := clientToDatasets[payload.Accounts[0].Name]
	toReturn = fmt.Sprintf(toReturn + " FROM `%s.%s.events_*` where _TABLE_SUFFIX BETWEEN '%s' AND '%s' %s %s desc",ProjectInfo.Project,ProjectInfo.Dataset, sDateParsed, eDateParsed, groupBy, orderBy)

	return toReturn, nil
}


type accountInfo struct {
	Project string
	Dataset string
}

type Payload struct {
	Accounts []AccountData `json:"accounts"`
	Dimensions []string `json:"dimensions"`
	Metrics []string `json:"metrics"`
	Start_date string `json:"start_date"`
	End_date string `json:"end_date"`
}

type AccountData struct {
	Name string
	View_id string
	Property string
}