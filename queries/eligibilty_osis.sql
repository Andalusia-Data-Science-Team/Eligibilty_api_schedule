SELECT distinct
        PE.PATIENT_ID,
        PE.episode_no,
        PE.START_DATE,
        nvl(PE.end_date,PE.START_DATE) as end_date,
        PM.PAT_NAME_1 ||' '|| PM.PAT_NAME_2 ||' '|| PM.PAT_NAME_FAMILY AS Patient_Name,
        PM.PAT_NAME_FAMILY AS Family_Name,
        PM.PAT_NAME_1 ,
        PM.PAT_NAME_2,
        pm.date_of_birth,
        CASE WHEN PM.SEX ='M' THEN 'male'
             WHEN PM.SEX ='F' THEN 'female'
             ELSE NULL END AS  Gender,
        CASE WHEN PM.NATIONALITY_CODE = 16 THEN 'NI'
        ELSE 'PRC' END AS NATIONALITY ,
        MA.description AS Marital,
        CASE WHEN MA.USER_CODE ='S' THEN 'U'
        ELSE MA.USER_CODE END AS Marital_CHAR,
        OC.description AS Occupation,
        PID.ID_NUMBER AS Iqama_NO,        
        p.purchaser_code AS INSURER,    
        P.nphies_license AS PAYER_LINCES,
        p.description AS PURCHASER_NAME,
        NVL(PO.nphies_license, P.nphies_license ) AS PAYER_LINCES_2
--------------------------------------------------------------Episode-----------------------------------------------------------
       
  FROM OASIS.PATIENT_EPISODES PE   -- TO GET START & END DATE
    LEFT JOIN OASIS.delivery_charge D  -- MAIN DATA
        ON PE.PATIENT_ID=D.PATIENT_ID AND PE.episode_no = D.episode_no

--------------------------------------------------------------Patient -----------------------------------------------------------

LEFT JOIN OASIS.patient_master_data PM
    ON PM.PATIENT_ID=D.PATIENT_ID
--------------------------------------------------------------Patient IQAMA-----------------------------------------------------------
    LEFT JOIN(
                SELECT PID.patient_id,PID.ID_NUMBER,
                        ROW_NUMBER() OVER(PARTITION BY PID.patient_id ORDER BY pid.id_when_expired DESC) AS RN
                FROM patient_ids PID
                WHERE PID.ID_TYPE_CODE IN( 1270, 68,243)
             )PID
        ON pm.PATIENT_ID=PID.PATIENT_ID
--------------------------------------------------------------Marital-----------------------------------------------------------
LEFT JOIN  CODES MA
    ON MA.CODE=pm.marital_code AND MA.code_type=2
   
--------------------------------------------------------------Occupation-----------------------------------------------------------
LEFT JOIN  CODES OC
    ON OC.CODE=pm.occupation_code    
--------------------------------------------------------------purhaser-----------------------------------------------------------
LEFT JOIN purchasers P
    ON p.purchaser_code=d.purchaser_code
--------------------------------------------------------------policy-----------------------------------------------------------
 LEFT JOIN policies PO
    ON PO.POLICY_CODE=D.POLICY_CODE
LEFT JOIN DOCL DL
    ON D.docl_id=DL.line_id
 
   
WHERE
    D.doc_id IS NOT NULL
    AND NVL(cancel_flag, 'X') NOT IN ('R', 'C')
    AND (D.package_id IS NULL OR D.package_deal_flag IS NULL)
    AND  D.payment_type IS NULL and NVL(D.purchaser_code,0)!=0

AND PE.START_DATE>= to_date ('1/' || to_char(sysdate,'MM')||'/'||to_char(sysdate,'yyyy'),'DD/MM/YYYY')  --THE EPISODES START DATE OD THE CURRENT MONTH
AND DL.amend_last_date  >= SYSDATE - INTERVAL '4' HOUR AND DL.amend_last_date < SYSDATE
--AND d.purchaser_code=183   --182 NCCI

--and pe.patient_id=946515    and pe.episode_no=82