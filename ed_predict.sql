DECLARE @sql nvarchar(max);
SELECT
	@sql = isnull(@sql+';', '') + 'DROP TABLE ' + SUBSTRING(name, 1, CHARINDEX( '____', name)-1)
FROM
	tempdb..sysobjects
WHERE
	name LIKE '#%'
	and
	name not like '##%'
	AND
	OBJECT_ID('tempdb..' + name) IS NOT NULL;

execute(@sql);

SELECT
	PAT_ENC_CSN_ID
INTO
	#PatientList
FROM
	Clarity.dbo.PAT_ENC_HSP
WHERE
	ED_EPISODE_ID IS NOT NULL 
	AND
	(
		ADMIT_CONF_STAT_C IS NULL 
		OR
		ADMIT_CONF_STAT_C NOT IN (2,3)
	) 
	AND
	ADT_ARRIVAL_TIME > = '2014-07-01'
	AND
	PAT_ENC_CSN_ID IN ('1105719180', '1107541427');


WITH NodeTimeDelta AS (
	SELECT
		STATUS_ID =  ROW_NUMBER() OVER (PARTITION BY PAT_ENC_CSN_ID ORDER BY STATUS_TIME, LINE),
		Status_List.SOURCE_TYPE,
		Status_List.PAT_ENC_CSN_ID,
		Status_List.STATUS_TIME,
		Status_List.NAME
	FROM (
		SELECT
			SOURCE_TYPE =		'PAT',
			PAT_ENC_CSN_ID =	ED_PAT_STATUS.PAT_ENC_CSN_ID,
			LINE =				ROW_NUMBER() OVER (PARTITION BY ED_PAT_STATUS.PAT_ENC_CSN_ID ORDER BY ED_PAT_STATUS.LINE ASC),
			STATUS_TIME =		ED_PAT_STATUS.PAT_STATUS_TIME,
			NAME =				ZC_ED_PAT_STATUS.NAME
		FROM
			#PatientList
				INNER JOIN
			Clarity.dbo.ED_PAT_STATUS
				ON #PatientList.PAT_ENC_CSN_ID = ED_PAT_STATUS.PAT_ENC_CSN_ID
				LEFT JOIN
			Clarity.dbo.ZC_ED_PAT_STATUS
				ON ED_PAT_STATUS.ED_PAT_STATUS_C = ZC_ED_PAT_STATUS.ED_PAT_STATUS_C

		UNION

		SELECT
			SOURCE_TYPE =		'LAB',
			PAT_ENC_CSN_ID =	ED_LAB_STATUS.PAT_ENC_CSN_ID,
			LINE =				ROW_NUMBER() OVER (PARTITION BY ED_LAB_STATUS.PAT_ENC_CSN_ID ORDER BY ED_LAB_STATUS.LINE ASC),
			STATUS_TIME =		ED_LAB_STATUS.LAB_STATUS_TIME,
			NAME =				ZC_ED_LAB_STATUS.NAME
		FROM
			#PatientList
				INNER JOIN
			Clarity.dbo.ED_LAB_STATUS
				ON #PatientList.PAT_ENC_CSN_ID = ED_LAB_STATUS.PAT_ENC_CSN_ID
				LEFT JOIN
			Clarity.dbo.ZC_ED_LAB_STATUS
				ON ED_LAB_STATUS.ED_LAB_STATUS_C = ZC_ED_LAB_STATUS.ED_LAB_STATUS_C

		UNION
		
		SELECT
			SOURCE_TYPE =		'RAD',
			PAT_ENC_CSN_ID =	ED_RAD_STATUS.PAT_ENC_CSN_ID,
			LINE =				ROW_NUMBER() OVER (PARTITION BY ED_RAD_STATUS.PAT_ENC_CSN_ID ORDER BY ED_RAD_STATUS.LINE ASC),
			STATUS_TIME =		ED_RAD_STATUS.RAD_STATUS_TIME,
			NAME =				ZC_ED_RAD_STATUS.NAME
		FROM
			#PatientList
				INNER JOIN
			Clarity.dbo.ED_RAD_STATUS
				ON #PatientList.PAT_ENC_CSN_ID = ED_RAD_STATUS.PAT_ENC_CSN_ID
				LEFT JOIN
			Clarity.dbo.ZC_ED_RAD_STATUS
				ON ED_RAD_STATUS.ED_RAD_STATUS_C = ZC_ED_RAD_STATUS.ED_RAD_STATUS_C

		UNION
		
		SELECT
			SOURCE_TYPE =		'CON',
			PAT_ENC_CSN_ID =	ED_CONSULT_STATUS.PAT_ENC_CSN_ID,
			LINE =				ROW_NUMBER() OVER (PARTITION BY ED_CONSULT_STATUS.PAT_ENC_CSN_ID ORDER BY ED_CONSULT_STATUS.LINE ASC),
			STATUS_TIME =		ED_CONSULT_STATUS.CONS_STATUS_TIME,
			NAME =				ZC_ED_CONS_STATUS.NAME
		FROM
			#PatientList
				INNER JOIN
			Clarity.dbo.ED_CONSULT_STATUS
				ON #PatientList.PAT_ENC_CSN_ID = ED_CONSULT_STATUS.PAT_ENC_CSN_ID
				LEFT JOIN
			Clarity.dbo.ZC_ED_CONS_STATUS
				ON ED_CONSULT_STATUS.ED_CONS_STATUS_C = ZC_ED_CONS_STATUS.ED_CONS_STATUS_C
	) Status_List
)
SELECT
	*
INTO
	#Statuses
FROM
	NodeTimeDelta;

WITH EventString AS (
	SELECT
		EVENT_ID =			ROW_NUMBER() OVER(PARTITION BY ED_IEV_PAT_INFO.PAT_ENC_CSN_ID ORDER BY ED_IEV_EVENT_INFO.EVENT_TIME),
		PAT_ENC_CSN_ID =	ED_IEV_PAT_INFO.PAT_ENC_CSN_ID,
		EVENT_TIME =		ED_IEV_EVENT_INFO.EVENT_TIME,
		EVENT_NAME =		ED_EVENT_TMPL_INFO.RECORD_NAME
	FROM
		ED_IEV_EVENT_INFO
			INNER JOIN
		ED_IEV_PAT_INFO
			ON ED_IEV_EVENT_INFO.EVENT_ID = ED_IEV_PAT_INFO.EVENT_ID
			LEFT JOIN
		ED_EVENT_TMPL_INFO
			ON ED_IEV_EVENT_INFO.EVENT_TYPE = ED_EVENT_TMPL_INFO.RECORD_ID
			INNER JOIN
		#PatientList
			ON ED_IEV_PAT_INFO.PAT_ENC_CSN_ID = #PatientList.PAT_ENC_CSN_ID
)
SELECT
	*
INTO
	#Events
FROM
	EventString;

CREATE CLUSTERED INDEX EventOrdering ON #Events (PAT_ENC_CSN_ID, EVENT_ID)
ALTER TABLE #Events ADD EVENT_STRING NVARCHAR(MAX);

SET NOCOUNT ON;

DECLARE @PreviousCSN INT;
DECLARE @PrevEventId INT;
DECLARE @EventRunningTotal NVARCHAR(MAX);

UPDATE
	#Events
SET
	@EventRunningTotal = EVENT_STRING =	CASE
											WHEN @PreviousCSN <> PAT_ENC_CSN_ID THEN EVENT_NAME
											WHEN EVENT_ID = @PrevEventId + 1 THEN @EventRunningTotal + CASE WHEN EVENT_NAME IS NOT NULL THEN '::' + EVENT_NAME ELSE '' END
                                            ELSE EVENT_NAME
										END,
	@PrevEventId = EVENT_ID,
	@PreviousCSN = PAT_ENC_CSN_ID
FROM
	#Events
WITH (TABLOCKX)
OPTION (MAXDOP 1);

SELECT
	Statuses.PAT_ENC_CSN_ID,
	Statuses.STATUS_ID,
	EVENT_ID =	MAX(EVENT_ID)
INTO
	#FinalEvent
FROM
	#Statuses Statuses
		LEFT JOIN
	#Events EventLog
		ON Statuses.STATUS_TIME >= EventLog.EVENT_TIME AND Statuses.PAT_ENC_CSN_ID = EventLog.PAT_ENC_CSN_ID
GROUP BY
	Statuses.PAT_ENC_CSN_ID,
	Statuses.STATUS_ID

SELECT
	StatusID =				ROW_NUMBER() OVER(PARTITION BY CurrentNode.PAT_ENC_CSN_ID ORDER BY CurrentNode.STATUS_ID),
	PAT_ENC_CSN_ID =		CurrentNode.PAT_ENC_CSN_ID,
	SourceSystem =			PreviousNode.SOURCE_TYPE,
	OriginNode =			PreviousNode.NAME,
	TargetNode =			CurrentNode.NAME,
	TimeDifference =		DATEDIFF(MINUTE, PreviousNode.STATUS_TIME, CurrentNode.STATUS_TIME),
	DepartingOriginTime =	CurrentNode.STATUS_TIME,
	ChiefComplaint =		CL_RSN_FOR_VISIT.REASON_VISIT_NAME,
	MeansOfArrival =		ZC_ARRIV_MEANS.NAME,
	EventLog =				EventLog.EVENT_STRING
INTO
	#EDLog
from
	#Statuses CurrentNode
		INNER JOIN
	#Statuses PreviousNode
		ON CurrentNode.STATUS_ID = PreviousNode.STATUS_ID + 1 AND CurrentNode.PAT_ENC_CSN_ID = PreviousNode.PAT_ENC_CSN_ID
		LEFT JOIN
	#FinalEvent FinalEvent
		on CurrentNode.STATUS_ID = FinalEvent.STATUS_ID AND CurrentNode.PAT_ENC_CSN_ID = FinalEvent.PAT_ENC_CSN_ID
		left join
	#Events EventLog
		on FinalEvent.EVENT_ID = EventLog.EVENT_ID AND FinalEvent.PAT_ENC_CSN_ID = EventLog.PAT_ENC_CSN_ID
		LEFT JOIN
	Clarity.dbo.PAT_ENC_RSN_VISIT
		ON CurrentNode.PAT_ENC_CSN_ID = PAT_ENC_RSN_VISIT.PAT_ENC_CSN_ID
		LEFT JOIN
	Clarity.dbo.CL_RSN_FOR_VISIT
		ON PAT_ENC_RSN_VISIT.ENC_REASON_ID = CL_RSN_FOR_VISIT.REASON_VISIT_ID
		LEFT JOIN
	Clarity.dbo.PAT_ENC_HSP
		ON CurrentNode.PAT_ENC_CSN_ID = PAT_ENC_HSP.PAT_ENC_CSN_ID
		LEFT JOIN
	Clarity.dbo.ZC_ARRIV_MEANS
		ON PAT_ENC_HSP.MEANS_OF_ARRV_C = ZC_ARRIV_MEANS.MEANS_OF_ARRV_C

select
	StatusID =				LastStatus.StatusID + 1,
	PAT_ENC_CSN_ID =		LastStatus.PAT_ENC_CSN_ID,
	SourceSystem =			'ADT',
	OriginNode =			#EDLog.TargetNode,
	TargetNode =			ZC_ED_DISPOSITION.NAME,
	TimeDifference =		DATEDIFF(MINUTE, #EDLog.DepartingOriginTime, ED_DISP_TIME),
	DepartingOriginTime =	ED_DISP_TIME,
	ChiefComplaint =		#EDLog.ChiefComplaint,
	MeansOfArrival =		#EDLog.MeansOfArrival,
	EventLog =				#EDLog.EventLog + '::DISPOSITION SET'
INTO
	#Disposition
from
	#EDLog
		INNER JOIN
	(
		SELECT
			PAT_ENC_CSN_ID,
			StatusID =	MAX(StatusID)
		FROM
			#EDLog
		GROUP BY
			PAT_ENC_CSN_ID
	) LastStatus
		ON #EDLog.PAT_ENC_CSN_ID = LastStatus.PAT_ENC_CSN_ID AND #EDLog.StatusID = LastStatus.StatusID
		LEFT JOIN
	PAT_ENC_HSP
		ON LastStatus.PAT_ENC_CSN_ID = PAT_ENC_HSP.PAT_ENC_CSN_ID
		LEFT JOIN
	ZC_ED_DISPOSITION
		ON PAT_ENC_HSP.ED_DISPOSITION_C = ZC_ED_DISPOSITION.ED_DISPOSITION_C


SELECT
	PAT_ENC_CSN_ID,
	OriginNode,
	TargetNode,
	TimeDifference,
	DepartingOriginTime,
	ChiefComplaint,
	MeansOfArrival,
	EventLog = (SELECT
					SUBSTRING((
						SELECT
							DISTINCT N'::' + item
						FROM
							RW.dbo.DataScienceSplit(EventLog, '::') AS [text()]
						FOR XML PATH ('')), 3, 2000))
FROM
(

	SELECT
		*
	FROM
		#EDLog

	UNION ALL

	SELECT
		*
	FROM
		#Disposition

	UNION ALL

	SELECT
		StatusID =				#Disposition.StatusID + 1,
		PAT_ENC_CSN_ID =		#Disposition.PAT_ENC_CSN_ID,
		SourceSystem =			'ADT',
		OriginNode =			#Disposition.TargetNode,
		TargetNode =			'ED Departure',
		TimeDifference =		DATEDIFF(MINUTE, #Disposition.DepartingOriginTime, ED_DEPARTURE_TIME),
		DepartingOriginTime =	ED_DEPARTURE_TIME,
		ChiefComplaint =		#Disposition.ChiefComplaint,
		MeansOfArrival =		#Disposition.MeansOfArrival,
		EventLog =				#Disposition.EventLog + '::DEPART ED'
	FROM
		#Disposition
			LEFT JOIN
		PAT_ENC_HSP
			ON #Disposition.PAT_ENC_CSN_ID = PAT_ENC_HSP.PAT_ENC_CSN_ID
) All_Statuses
ORDER BY
	PAT_ENC_CSN_ID, StatusID