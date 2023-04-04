from django.db import models

# Template to load data from excel or csv
class PlantillaCarga(models.Model):
    nombre = models.CharField(max_length=30,null=True,blank=True) #name
    desc = models.CharField(max_length=300,null=True,blank=True)
    status = models.BooleanField(default=True)
    add_date = models.DateField(auto_now_add=True)

# Template Field
class PlantillaField(models.Model):

    nombre = models.CharField(max_length=20,null=False,blank=False) # name of the col in the csv or excel
    field = models.CharField(max_length=40,null=False,blank=False) # name of the field in the database
    plantilla = models.ForeignKey(PlantillaCarga,on_delete=models.CASCADE,null=False)
    position = models.IntegerField(null=True)

    class Meta:
        ordering = ('position','id')

    
@task(name='load_contacts')
def load_contacts(df,request):
    plantilla = PlantillaCarga.objects.filter(id=int(request.data['template'])).first()
    p_fields = PlantillaField.objects.filter(plantilla=plantilla)

    invalidas = 0
    validas = 0
    creadas = 0
    actualizadas = 0
    contactos = []
    error_rows = []

    # get a pk field to identify the Contact Model
    identifier_field = p_fields.filter(field='nro_cuenta')
    print(identifier_field) 
    if identifier_field.count() > 0:
        identifier_field = identifier_field.first().nombre
    else:
        identifier_field = 'nro_cuenta'

    nums_col = p_fields.filter(field='numeros')
    if nums_col.count() > 0:
        nums_col = nums_col.first().nombre
    else:
        nums_col = "numeros"
    emails_col = p_fields.filter(field='emails')
    if emails_col.count() > 0:
        emails_col = emails_col.first().nombre
    else:
        emails_col = "emails"

    # fix pk if has ceros before the first digit
    df[identifier_field] = df[identifier_field].apply(lambda x: str(x).zfill(int(request.data['digitos'])))


    for row in df.to_dict('records'):
        row_copy = row
        try:
            with transaction.atomic():
                # get the contact by the pk field
                q = Contacto.objects.filter(nro_cuenta=row[identifier_field])
                # check if the contact exists
                if q.count() == 0:

                    # fix colums containing phone numbers or email address
                    try:
                        numeros = row[nums_col].split(',')
                    except:
                        numeros = ""
                    try:
                        emails = row[emails_col].split(',')
                    except:
                        emails = ""
                    if 'numeros' in df.columns:
                        del row['numeros']
                    if 'emails' in df.columns:
                        del row['emails']
                    
                    # ---------------

                    data = {} 
                    # convert the data from the row to a new dict with the corresponding database names 
                    for col in row.keys():
                        f = p_fields.filter(nombre=col)
                        if f.count() > 0:
                            f = f.first()
                            data[f.field] = row[col]
                        else:
                            continue
                        
                    # validate creation to return errors in a new excel file
                    contact = Contacto(**data)
                    try:
                        contact.full_clean()
                    except Exception as e:
                        errors = e.message_dict
                        row_copy['errors'] = errors 
                        error_rows.append(row_copy)
                        invalidas += 1
                        continue

                    # if the data is valid create the contact 
                    contact = Contacto.objects.create(**data)
                    
                    # create email address for the contact
                    if emails not in [None,"","NULL"]:
                        for e in emails:
                            try:
                                e_data = {'contacto':contact} 
                                addr = e.split(':')
                                if len(addr) > 1:
                                    e_data['verification'] = int(e.split(':')[-1])

                                e_data['address'] = e.split(':')[0]
                                Email.objects.create(**e_data)
                            except:
                                continue

                    # create phone numbers and get posible data if provided that should be separated by ":"
                    if numeros != "":
                        for num in numeros:
                            try:
                                num_data = {'contacto':contact}
                                if len(num.split(":")) > 1:
                                    num_data['verified'] = int(num.split(':')[1])
                                num_data['numero'] = num.split(':')[0].strip()
                                if len(num.split(":")) > 2:
                                    num_data['external_id'] = int(num.split(':')[-1])
                                if len(num_data['numero']) == 9:
                                    num_data['numero'] = '+56' + num_data['numero']
                                if len(num_data['numero']) == 8:
                                    num_data['numero'] = '+569' + num_data['numero']
                                Numero.objects.create(**num_data)
                            except:
                                continue
            
                    contactos.append(contact)
                    validas += 1
                    creadas += 1

                else:
                    # if contact exists update

                    contact = q.first()
                    # fix phone numbers or emails adresses
                    try:
                        numeros = row[nums_col].split(',')
                    except:
                        numeros = ""
                    try:
                        emails = row[emails_col].split(',')
                    except Exception as e:
                        emails = ""
                    if 'numeros' in df.columns:
                        del row['numeros']
                    if 'emails' in df.columns:
                        del row['emails']
                    
                    data = {} 
                    for col in row.keys():
                        f = p_fields.filter(nombre=col)
                        if f.count() > 0:
                            f = f.first()
                            data[f.field] = row[col]
                        else:
                            continue


                    # create email adressses
                    if emails not in [None,"","NULL"]:
                        for e in emails:
                            try:
                                e_data = {'contacto':contact} 
                                addr = e.split(':')
                                if len(addr) > 1:
                                    e_data['verification'] = int(e.split(':')[-1])

                                e_data['address'] = e.split(':')[0]
                                Email.objects.create(**e_data)
                            except Exception as e:
                                print(e)
                                continue
                    # create phone numbers
                    if numeros != "":
                        for num in numeros:
                            try:
                                num_data = {'contacto':contact}
                                if len(num.split(":")) > 1:
                                    num_data['verified'] = int(num.split(':')[1])

                                num_data['numero'] = num.split(':')[0].strip()
                                
                                if len(num.split(":")) > 2:
                                    num_data['external_id'] = int(num.split(':')[-1])
                                
                                if len(num_data['numero']) == 9:
                                    num_data['numero'] = '+56' + num_data['numero']
                                if len(num_data['numero']) == 8:
                                    num_data['numero'] = '+569' + num_data['numero']
                                # check if phone number exists
                                if Numero.objects.filter(contacto=contact,numero=num_data['numero']).count() <= 0:
                                    Numero.objects.create(**num_data)

                            except:
                                continue

                    # update the Contact data
                    for attr, value in data.items():
                        setattr(contact, attr, value)
                    
                    # validate data
                    try:
                        contact.full_clean()
                    except Exception as e:
                        # if invalid it will return the row with the column erors in excel
                        errors = e.message_dict
                        row_copy['errors'] = errors 
                        error_rows.append(row_copy)
                        invalidas += 1
                        continue

                    contact.save()
                    validas += 1
                    actualizadas += 1

        except Exception as e:
            invalidas += 1 
            row_copy['errors'] = e
            error_rows.append(row_copy)
