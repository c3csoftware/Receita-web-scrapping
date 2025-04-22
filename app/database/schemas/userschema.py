from sqlalchemy import String, Integer, DateTime, Numeric, func, ForeignKey
from sqlalchemy.orm import registry, mapped_column, Mapped, relationship
from datetime import datetime

table_registry = registry()

@table_registry.mapped_as_dataclass
class Company: 
    __tablename__ = "Companies"

    base_cnpj: Mapped[str] = mapped_column(primary_key=True)
    social_reason_business_name: Mapped[str] = mapped_column(String(255), nullable=False)
    legal_nature: Mapped[str] = mapped_column(String(255))
    social_capital_company: Mapped[Numeric] = mapped_column(Numeric(15, 2), nullable=False)
    company_size: Mapped[str] = mapped_column(String(5))
    responsible_federative_entity: Mapped[str] = mapped_column(String(255))
    created_at: Mapped[datetime] = mapped_column(DateTime, init=False, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now(), onupdate=func.now())
    responsible_qualification: Mapped[str] = mapped_column(String(20), default="00")


@table_registry.mapped_as_dataclass
class Establishment:
    __tablename__ = 'Establishments'

    base_cnpj: Mapped[str] = mapped_column(String(255), nullable=True)
    cnpj_order: Mapped[str] = mapped_column(String(4), nullable=False)
    cnpj_dv: Mapped[str] = mapped_column(String(4), nullable=False)
    identifier_branch_matriz: Mapped[str] = mapped_column(String(255), nullable=True)
    cadastral_situation: Mapped[str] = mapped_column(String(255), nullable=True)
    cadastral_situation_reason: Mapped[str] = mapped_column(String(255), nullable=True)
    fantasy_name: Mapped[str] = mapped_column(String(255), nullable=True)
    city_name_exterior: Mapped[str] = mapped_column(String(255), nullable=True)
    country: Mapped[str] = mapped_column(String(255), nullable=True)
    activity_start_date: Mapped[datetime] = mapped_column(DateTime, nullable=True)
    cnae_main: Mapped[str] = mapped_column(String(255), nullable=True)
    cnpj: Mapped[str] = mapped_column(primary_key=True)
    street_type: Mapped[str] = mapped_column(String(255), nullable=True)
    street: Mapped[str] = mapped_column(String(255), nullable=True)
    number: Mapped[str] = mapped_column(String(10), nullable=True)
    complement: Mapped[str] = mapped_column(String(255), nullable=True)
    neighborhood: Mapped[str] = mapped_column(String(255))
    cep: Mapped[str] = mapped_column(String(15), nullable=True)
    city: Mapped[str] = mapped_column(String(255), nullable=True)
    ddd_1: Mapped[str] = mapped_column(String(255), nullable=True)
    phone_1: Mapped[str] = mapped_column(String(22), nullable=True)
    ddd_2: Mapped[str] = mapped_column(String(255), nullable=True)
    phone_2: Mapped[str] = mapped_column(String(22), nullable=True)
    fax_ddd: Mapped[str] = mapped_column(String(255), nullable=True)
    fax: Mapped[str] = mapped_column(String(22), nullable=True)
    electronic_mail: Mapped[str] = mapped_column(String(255), nullable=True)
    special_situation: Mapped[str] = mapped_column(String(255), nullable=True)
    special_situation_date: Mapped[datetime] = mapped_column(DateTime, nullable=True)
    uf: Mapped[str] = mapped_column(String(255), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, init=False, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now(), onupdate=func.now())
    cnae_secondary: Mapped[str] = mapped_column(nullable=True, default=None)


@table_registry.mapped_as_dataclass
class Simples:
    __tablename__ = 'Simples'

    base_cnpj: Mapped[str] = mapped_column(String(20), nullable=False, primary_key=True)
    simples_option: Mapped[str] = mapped_column(String(4))
    simples_option_date: Mapped[datetime] = mapped_column(DateTime, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, init=False, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now(), onupdate=func.now())
    mei_option_date: Mapped[datetime] = mapped_column(DateTime, nullable=True)
    simples_option_exclusion_date: Mapped[datetime] = mapped_column(DateTime, nullable=True, default=None)
    mei_option: Mapped[str] = mapped_column(String(4), nullable=True, default=None)
    mei_exclusion_date: Mapped[datetime] = mapped_column(DateTime, nullable=True, default=None)


@table_registry.mapped_as_dataclass
class Partner:
    __tablename__ = 'Partners'

    base_cnpj: Mapped[str] = mapped_column(String(20), nullable=True)
    partner_identifier: Mapped[str] = mapped_column(String(4))
    partner_name_social_reason: Mapped[str] = mapped_column(String(255))
    partner_cpf_cnpj: Mapped[str] = mapped_column(String(14), primary_key=True)
    partner_qualification: Mapped[str] = mapped_column(String(20))
    date_entry_society: Mapped[datetime] = mapped_column(DateTime)
    country: Mapped[str] = mapped_column(String(20))
    cpf_legal_representative: Mapped[str] = mapped_column(String(11))
    representative_name: Mapped[str] = mapped_column(String(255))
    legal_representative_qualification: Mapped[str] = mapped_column(String(20))
    age_group: Mapped[str] = mapped_column(String(1))
    created_at: Mapped[datetime] = mapped_column(DateTime, init=False, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now(), onupdate=func.now())
    
@table_registry.mapped_as_dataclass
class LegalNature:
    __tablename__ = 'LegalNature'

    code: Mapped[str] = mapped_column(String(20), primary_key=True)  
    description: Mapped[str] = mapped_column(String(255), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, init=False, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now(), onupdate=func.now())

@table_registry.mapped_as_dataclass
class City:
    __tablename__ = 'Cities'

    code: Mapped[str] = mapped_column(String(20), primary_key=True)  
    description: Mapped[str] = mapped_column(String(255), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, init=False, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now(), onupdate=func.current_timestamp())

@table_registry.mapped_as_dataclass
class Country:
    __tablename__ = 'Countries'

    code: Mapped[str] = mapped_column(String(20), primary_key=True)  
    description: Mapped[str] = mapped_column(String(255), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, init=False, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now(), onupdate=func.now())
    
@table_registry.mapped_as_dataclass
class Cnae:
    __tablename__ = 'Cnae'

    code: Mapped[str] = mapped_column(String(20), primary_key=True)  
    description: Mapped[str] = mapped_column(String(255), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, init=False, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now(), onupdate=func.now())

@table_registry.mapped_as_dataclass
class PartnerQualification:
    __tablename__ = 'PartnerQualification'

    code: Mapped[str] = mapped_column(String(20), primary_key=True)  
    description: Mapped[str] = mapped_column(String(255), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, init=False, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now(), onupdate=func.now())


@table_registry.mapped_as_dataclass
class SituationMotive:
    # Qualificações de Sócios
    __tablename__ = 'SituationMotives'

    code: Mapped[str] = mapped_column(String(255), primary_key=True)  
    description: Mapped[str] = mapped_column(String(255), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, init=False, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now(), onupdate=func.now())

@table_registry.mapped_as_dataclass
class Client:
    #Aplicações clientes
    __tablename__ = 'Clients'
    
    id: Mapped[int] = mapped_column(primary_key=True)
    client_id: Mapped[str] = mapped_column(String(32), unique=True)
    client_secret: Mapped[str] = mapped_column(String(64)) 
    created_at: Mapped[datetime] = mapped_column(DateTime, init=False, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now(), onupdate=func.now())

