from sqlalchemy import String, Integer, DateTime, Numeric, func, ForeignKey
from sqlalchemy.orm import registry, mapped_column, Mapped, relationship
from datetime import datetime

table_registry = registry()
@table_registry.mapped_as_dataclass
class Company: 
    __tablename__ = "Companies"

    base_cnpj: Mapped[str] = mapped_column(String(20), nullable=False, primary_key=True)
    social_reason_business_name: Mapped[str] = mapped_column(String(255), nullable=False)
    legal_nature: Mapped[str] = mapped_column(String(255),ForeignKey("LegalNature.code"))
    responsible_qualification: Mapped[str] = mapped_column(ForeignKey("PartnerQualification.code"))
    social_capital_company: Mapped[Numeric] = mapped_column(Numeric(15, 2), nullable=False)
    company_size: Mapped[str] = mapped_column(String(5))
    responsible_federative_entity: Mapped[str] = mapped_column(String(255))
    created_at: Mapped[datetime] = mapped_column(DateTime, init=False, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now(), onupdate=func.now())

    # Relacionamento com estabelecimentos
    establishments: Mapped[list["Establishment"]] = relationship("Establishment", back_populates="company", default_factory=list)

@table_registry.mapped_as_dataclass
class Establishment:
    __tablename__ = 'Establishments'

    base_cnpj: Mapped[str] = mapped_column(ForeignKey("Companies.base_cnpj"), nullable=True)
    cnpj_order: Mapped[str] = mapped_column(String(4), nullable=False)
    cnpj_dv: Mapped[str] = mapped_column(String(4), nullable=False)
    identifier_branch_matriz: Mapped[str] = mapped_column(String(255), nullable=True)
    cadastral_situation: Mapped[str] = mapped_column(String(255), nullable=True)
    cadastral_situation_reason: Mapped[str] = mapped_column(String(255), nullable=True)
    fantasy_name: Mapped[str] = mapped_column(String(255), nullable=True)
    city_name_exterior: Mapped[str] = mapped_column(String(255), nullable=True)
    country: Mapped[str] = mapped_column(ForeignKey("Countries.code"))
    activity_start_date: Mapped[datetime] = mapped_column(DateTime, nullable=True)
    cnae_main: Mapped[str] = mapped_column(ForeignKey("Cnae.code"))
    cnpj: Mapped[str] = mapped_column(primary_key=True)
    street_type: Mapped[str] = mapped_column(String(255))
    street: Mapped[str] = mapped_column(String(255))
    number: Mapped[str] = mapped_column(String(10), nullable=True)
    complement: Mapped[str] = mapped_column(String(255), nullable=True)
    neighborhood: Mapped[str] = mapped_column(String(255))
    cep: Mapped[str] = mapped_column(String(15), nullable=True)
    city: Mapped[str] = mapped_column(ForeignKey("Cities.code"))
    ddd_1: Mapped[str] = mapped_column(String(20), nullable=True)
    phone_1: Mapped[str] = mapped_column(String(22), nullable=True)
    ddd_2: Mapped[str] = mapped_column(String(20), nullable=True)
    phone_2: Mapped[str] = mapped_column(String(22), nullable=True)
    fax_ddd: Mapped[str] = mapped_column(String(20), nullable=True)
    fax: Mapped[str] = mapped_column(String(22), nullable=True)
    electronic_mail: Mapped[str] = mapped_column(String(255), nullable=True)
    special_situation: Mapped[str] = mapped_column(String(255), nullable=True)
    special_situation_date: Mapped[datetime] = mapped_column(DateTime, nullable=True)
    uf: Mapped[str] = mapped_column(String(20), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, init=False, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now(), onupdate=func.now())

    # Relacionamento com Empresas
    company: Mapped["Company"] = relationship("Company", init=False, back_populates="establishments")
    cnae_secondary: Mapped[str] = mapped_column(nullable=True, default=None)
    cnae_main_rel: Mapped["Cnae"] = relationship("Cnae", foreign_keys=[cnae_main], backref="main_establishments", init=False)

@table_registry.mapped_as_dataclass
class Simples:
    # Simples
    __tablename__ = 'Simples'

    base_cnpj: Mapped[str] = mapped_column(ForeignKey("Companies.base_cnpj"), nullable=False, primary_key=True)  # Relacionamento com Empresas
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
    # Sócios
    __tablename__ = 'Partners'

    base_cnpj: Mapped[str] = mapped_column(ForeignKey("Companies.base_cnpj"), nullable=True)  # Relacionamento com Empresas
    partner_identifier: Mapped[str] = mapped_column(String(4))
    partner_name_social_reason: Mapped[str] = mapped_column(String(255))
    partner_cpf_cnpj: Mapped[str] = mapped_column(String(14), primary_key=True)
    partner_qualification: Mapped[str] = mapped_column(ForeignKey("PartnerQualification.code"))  # Relacionamento com Qualificação do Sócio, usando o code
    date_entry_society: Mapped[datetime] = mapped_column(DateTime)
    country: Mapped[str] = mapped_column(ForeignKey("Countries.code"))  # Relacionamento com Países, usando o code
    cpf_legal_representative: Mapped[str] = mapped_column(String(11))
    representative_name: Mapped[str] = mapped_column(String(255))
    legal_representative_qualification: Mapped[str] = mapped_column(ForeignKey("PartnerQualification.code"))  # Relacionamento com Qualificação do Representante, usando o code
    age_group: Mapped[str] = mapped_column(String(1))
    created_at: Mapped[datetime] = mapped_column(DateTime, init=False, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now(), onupdate=func.now())
    
@table_registry.mapped_as_dataclass
class LegalNature:
    # Naturezas Jurídicas
    __tablename__ = 'LegalNature'

    code: Mapped[str] = mapped_column(String(20), primary_key=True)  
    description: Mapped[str] = mapped_column(String(255), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, init=False, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now(), onupdate=func.now())

@table_registry.mapped_as_dataclass
class City:
    # Municípios
    __tablename__ = 'Cities'

    code: Mapped[str] = mapped_column(String(20), primary_key=True)  
    description: Mapped[str] = mapped_column(String(255), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, init=False, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now(), onupdate=func.current_timestamp())

@table_registry.mapped_as_dataclass
class Country:
    # Países
    __tablename__ = 'Countries'

    code: Mapped[str] = mapped_column(String(20), primary_key=True)  
    description: Mapped[str] = mapped_column(String(255), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, init=False, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now(), onupdate=func.now())
    
@table_registry.mapped_as_dataclass
class Cnae:
    # CNAEs
    __tablename__ = 'Cnae'

    code: Mapped[str] = mapped_column(String(20), primary_key=True)  
    description: Mapped[str] = mapped_column(String(255), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, init=False, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now(), onupdate=func.now())

@table_registry.mapped_as_dataclass
class PartnerQualification:
    # Qualificações de Sócios
    __tablename__ = 'PartnerQualification'

    code: Mapped[str] = mapped_column(String(20), primary_key=True)  
    description: Mapped[str] = mapped_column(String(255), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, init=False, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now(), onupdate=func.now())
