import apache_beam as beam
from functions import *


p = beam.Pipeline()

################################# CARDS ########################################
card_defaulter = (
                  p
                  | 'Read credit card data' >> beam.io.ReadFromText('cards.txt',skip_header_lines=1) #THE FIRST LINE OF THE FILE IS THE NAME OF THE COLUMNS. WE SKIP THEM.
                  | 'Calculate defaulter points' >> beam.Map(calculate_points) #each person has 12 lines,one per month. the output here is the default points for each month for a person.
                  | 'Combine points for defaulters' >> beam.CombinePerKey(sum)                            # key--> CT28383,Miyako Burns   value --> 6
                  | 'Filter card defaulters' >> beam.Filter(lambda element: element[1] > 0)   #we only want those who missed at least a payment. Element 1 is the sum of all default points.
                  | 'Format output' >> beam.Map(format_result)                                            # CT28383,Miyako Burns,6 fraud_points
                  | 'Write credit card data' >> beam.io.WriteToText('outputs/card_skippers')
                  #| 'tuple ' >> beam.Map(return_tuple) #WE NEED THIS FOR THE CoGroupByKey at the end. they have to be in the format Key,value
                  )


################################# LOANS #######################################

medical_loan_defaulter = (
                            p
                            |  beam.io.ReadFromText('loan.txt',skip_header_lines=1)
                            | 'Split Row' >> beam.Map(lambda row : row.split(',')) # 1stRow--> CT88330,Humberto,Banks,Serviceman,LN_1559,Medical Loan,26-01-2018, 2000, 30-01-2018
                            | 'Filter medical loan' >> beam.Filter(lambda element : (element[5]).rstrip().lstrip() == 'Medical Loan')
                            | 'Calculate late payment' >> beam.Map(calculate_late_payment) # output ->[CT88330,Humberto,Banks,Serviceman,LN_1559,Medical Loan,26-01-2018,2000,30-01-2018,1]
                            | 'Make key value pairs' >> beam.Map(lambda elements: (elements[0] + ', ' + elements[1]+' '+elements[2], int(elements[9])) ) #element 9 comes from calculate_late_payment. 0 for ok, 1 for late
                            | 'Group medical loan based on month' >> beam.CombinePerKey(sum) # combines previous line into key--> (CT88330,Humberto Banks)  value --> 7
                            | 'Check for medical loan defaulter' >> beam.Filter(lambda element: element[1] >= 3) #3 or more late payments is considered default.
                            | 'Format medical loan output' >> beam.Map(format_output)      # CT88330,Humberto Banks,7 missed
                         )

personal_loan_defaulter = (
                            p
                            | 'Read' >> beam.io.ReadFromText('loan.txt',skip_header_lines=1)
                            | 'Split' >> beam.Map(lambda row : row.split(','))
                            | 'Filter personal loan' >> beam.Filter(lambda element : (element[5]).rstrip().lstrip() == 'Personal Loan')
                            | 'Split and Append New Month Column' >> beam.Map(calculate_month)
                            | 'Make key value pairs loan' >> beam.Map(lambda elements: (elements[0] + ', ' + elements[1]+' '+elements[2], int(elements[9])) )
                            | 'Group personal loan based on month' >> beam.GroupByKey()# CT68554,Ronald Chiki [01,05,06,07,08,09,10,11,12] we get a list with all the months were there was a payment.
                            | 'Check for personal loan defaulter' >> beam.Map(calculate_personal_loan_defaulter)          # CT68554,Ronald Chiki   3
                            | 'Filter only personal loan defaulters' >> beam.Filter(lambda element: element[1] > 0)
                            | 'Format personal loan output' >> beam.Map(format_output)        # CT68554,Ronald Chiki,3 missed
                          )


final_loan_defaulters = (
                          ( personal_loan_defaulter, medical_loan_defaulter )
                          | 'Combine all defaulters' >> beam.Flatten()
                          | 'Write all defaulters to text file' >> beam.io.WriteToText('outputs/loan_defaulters')
                          #| 'tuple for loan' >> beam.Map(return_tuple) #WE NEED THIS FOR THE CoGroupByKey at the end. they have to be in the format Key,value
                        )

################################# CARDS & LOANS #######################################

'''both_defaulters =  (
                    {'card_defaulter': card_defaulter, 'loan_defaulter': final_loan_defaulters}
                    | beam.CoGroupByKey()
                    |'Write p3 results' >> beam.io.WriteToText('outputs/both')
                   )'''


p.run()
