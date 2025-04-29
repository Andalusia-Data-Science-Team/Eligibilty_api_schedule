SELECT  
            V.ID AS visit_id ,
            V.CreatedDate AS start_date,
        V.UpdatedDate AS end_date,
            P.ID AS patient_id,
            CONVERT(DATE,P.DateOfBirth) AS date_of_birth,
            P.OccupationID,
            OCC.EnName AS Occupation,
            CONCAT(P.EnFirstName, ' ', P.EnSecondName, ' ', P.EnThridName, ' ', P.EnLastName) AS patient_name,
            P.EnLastName AS family_name,
            P.EnFirstName AS pat_name_1,
            P.EnSecondName AS pat_name_2,
            G.EnName AS gender,
            P.NationalityID,
            MS.EnName AS marital_char,
            CASE WHEN P.NationalityID=56 THEN 'NI'
                   WHEN P.NationalityID !=56 AND P.IdentificationTypeID =2 THEN 'PPN'
                   ELSE 'PRC' END AS nationality,
            P.IdentificationValue AS iqama_no,
            1 AS  Organization_Code,
                  'ANDALUSIA HOSPITAL JEDDAH -  PROD' AS 'Organization Name',
            10000000046019 AS 'provider-license',

            vfi.ContractorClientPolicyNumber,
            vfi.ContractorCode AS insurer,
            vfi.InsuranceCardNo,
            vfi.ContractorClientEnName,
            vfi.ContractorClientID,
            CGWM.EnName AS purchaser_name,
            CGWM.Code AS payer_linces

            


FROM VisitMgt.Visit AS v
LEFT JOIN VisitMgt.VisitFinincailInfo AS vfi
      ON vfi.VisitID = v.id
LEFT JOIN   MPI.Patient P
      on p.id=v.patientid
LEFT JOIN MPI.SLKP_Occupation OCC
      ON P.OccupationID=OCC.ID
LEFT JOIN MPI.SLKP_Gender G
      ON P.GenderID=G.ID
LEFT JOIN MPI.SLKP_MaritalStatus MS
      ON P.MaritalStatusID=MS.ID
LEFT JOIN Billing.ContractorGateWayMappings CGWM
      ON CGWM.ContractorID=VFI.ContractorID AND CGWM.GateWayID=3

WHERE  CONVERT(DATE,V.CreatedDate) >= DATEADD(MONTH, DATEDIFF(MONTH, 0, GETDATE()), 0) ---FROM THE FIRST OF THE MONTH 
--AND CONVERT(DATE,V.CreatedDate)=CONVERT(DATE,GETDATE()) --FOR TODATY ONLY 
--AND  CONVERT(DATETIME,V.CreatedDate) >= DATEADD(HOUR, -4, GETDATE())   --LAST 4 HOURS