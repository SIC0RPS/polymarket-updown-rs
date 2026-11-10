use super::*;
use yaml_rust2::YamlLoader;

const PNL_UNITS_PER_DOLLAR: i128 = 1_000_000;

#[derive(Clone, Debug)]
pub(crate) struct ObjectiveProfile {
    bytes: Vec<u8>,
    formula_type: String,
    expression: String,
    sha256: String,
    parsed: Expression,
}

impl ObjectiveProfile {
    pub(crate) fn load(path: &Path) -> Result<Self> {
        let bytes =
            fs::read(path).with_context(|| format!("read objective profile {}", path.display()))?;
        Self::from_bytes(bytes)
            .with_context(|| format!("parse objective profile {}", path.display()))
    }

    pub(crate) fn from_bytes(bytes: Vec<u8>) -> Result<Self> {
        let content = std::str::from_utf8(&bytes).context("objective profile is not UTF-8")?;
        let documents =
            YamlLoader::load_from_str(content).context("parse objective profile YAML")?;
        ensure!(
            documents.len() == 1,
            "objective profile must contain one YAML document"
        );
        let document = &documents[0];
        let fields = document
            .as_hash()
            .context("objective profile must be a YAML mapping")?;
        ensure!(
            fields.len() == 2,
            "objective profile must contain only formula_type and expression"
        );
        let formula_type = document["formula_type"]
            .as_str()
            .context("objective profile formula_type must be a string")?;
        ensure!(
            formula_type == "expression",
            "objective profile formula_type must be expression"
        );
        let expression = document["expression"]
            .as_str()
            .context("objective profile expression must be a string")?
            .to_string();
        ensure!(
            !expression.trim().is_empty(),
            "objective profile expression is empty"
        );
        let parsed = Parser::new(&expression).parse()?;
        let sha256 = format!("{:x}", Sha256::digest(&bytes));
        Ok(Self {
            bytes,
            formula_type: formula_type.to_string(),
            expression,
            sha256,
            parsed,
        })
    }

    pub(crate) fn frozen_bytes(&self) -> &[u8] {
        &self.bytes
    }

    pub(crate) fn expression(&self) -> &str {
        &self.expression
    }

    pub(crate) fn formula_type(&self) -> &str {
        &self.formula_type
    }

    pub(crate) fn sha256(&self) -> &str {
        &self.sha256
    }

    pub(crate) fn score(
        &self,
        total_pnl_units_6dp: i128,
        realized_pnl_units_6dp: i128,
        entered_markets: u64,
        profitable_entered_markets: u64,
    ) -> Result<scoring::ObjectiveScore> {
        ensure!(
            profitable_entered_markets <= entered_markets,
            "profitable market count exceeds entered market count"
        );
        let pnl = Exact::integer(total_pnl_units_6dp);
        if entered_markets == 0 {
            return Ok(pnl.into());
        }
        let entries = i128::from(entered_markets);
        let average_denominator = entries
            .checked_mul(PNL_UNITS_PER_DOLLAR)
            .context("objective average PnL denominator overflow")?;
        let inputs = Inputs {
            pnl,
            average_realized_pnl: Exact::new(realized_pnl_units_6dp, average_denominator)?,
            profitable_markets: Exact::integer(i128::from(profitable_entered_markets)),
            entered_markets: Exact::integer(entries),
        };
        Ok(self.parsed.evaluate(&inputs)?.into())
    }
}

#[derive(Clone, Copy, Debug)]
struct Exact {
    numerator: i128,
    denominator: i128,
}

impl Exact {
    fn integer(value: i128) -> Self {
        Self {
            numerator: value,
            denominator: 1,
        }
    }

    fn new(numerator: i128, denominator: i128) -> Result<Self> {
        ensure!(
            denominator > 0,
            "objective expression denominator must be positive"
        );
        if numerator == 0 {
            return Ok(Self::integer(0));
        }
        Ok(Self {
            numerator,
            denominator,
        })
    }

    fn add(self, other: Self) -> Result<Self> {
        if self.numerator == 0 {
            return Ok(other);
        }
        if other.numerator == 0 {
            return Ok(self);
        }
        let left = self
            .numerator
            .checked_mul(other.denominator)
            .context("objective expression addition overflow")?;
        let right = other
            .numerator
            .checked_mul(self.denominator)
            .context("objective expression addition overflow")?;
        Self::new(
            left.checked_add(right)
                .context("objective expression addition overflow")?,
            self.denominator
                .checked_mul(other.denominator)
                .context("objective expression denominator overflow")?,
        )
    }

    fn subtract(self, other: Self) -> Result<Self> {
        if other.numerator == 0 {
            return Ok(self);
        }
        let left = self
            .numerator
            .checked_mul(other.denominator)
            .context("objective expression subtraction overflow")?;
        let right = other
            .numerator
            .checked_mul(self.denominator)
            .context("objective expression subtraction overflow")?;
        Self::new(
            left.checked_sub(right)
                .context("objective expression subtraction overflow")?,
            self.denominator
                .checked_mul(other.denominator)
                .context("objective expression denominator overflow")?,
        )
    }

    fn multiply(self, other: Self) -> Result<Self> {
        if self.numerator == 0 || other.numerator == 0 {
            return Ok(Self::integer(0));
        }
        Self::new(
            self.numerator
                .checked_mul(other.numerator)
                .context("objective expression product overflow")?,
            self.denominator
                .checked_mul(other.denominator)
                .context("objective expression denominator overflow")?,
        )
    }

    fn divide(self, other: Self) -> Result<Self> {
        ensure!(
            other.numerator != 0,
            "objective expression division by zero"
        );
        if self.numerator == 0 {
            return Ok(Self::integer(0));
        }
        let numerator = self
            .numerator
            .checked_mul(other.denominator)
            .context("objective expression quotient overflow")?;
        let denominator = self
            .denominator
            .checked_mul(other.numerator)
            .context("objective expression quotient overflow")?;
        if denominator < 0 {
            Self::new(
                numerator
                    .checked_neg()
                    .context("objective expression quotient overflow")?,
                denominator
                    .checked_neg()
                    .context("objective expression quotient overflow")?,
            )
        } else {
            Self::new(numerator, denominator)
        }
    }

    fn compare(self, other: Self) -> Result<std::cmp::Ordering> {
        let left = self
            .numerator
            .checked_mul(other.denominator)
            .context("objective expression comparison overflow")?;
        let right = other
            .numerator
            .checked_mul(self.denominator)
            .context("objective expression comparison overflow")?;
        Ok(left.cmp(&right))
    }
}

impl From<Exact> for scoring::ObjectiveScore {
    fn from(value: Exact) -> Self {
        Self {
            numerator: value.numerator,
            denominator: value.denominator,
        }
    }
}

struct Inputs {
    pnl: Exact,
    average_realized_pnl: Exact,
    profitable_markets: Exact,
    entered_markets: Exact,
}

#[derive(Clone, Debug)]
enum Expression {
    Number(i128),
    P,
    A,
    W,
    N,
    Add(Box<Self>, Box<Self>),
    Subtract(Box<Self>, Box<Self>),
    Multiply(Box<Self>, Box<Self>),
    Divide(Box<Self>, Box<Self>),
    Max(Box<Self>, Box<Self>),
}

impl Expression {
    fn evaluate(&self, inputs: &Inputs) -> Result<Exact> {
        match self {
            Self::Number(value) => Ok(Exact::integer(*value)),
            Self::P => Ok(inputs.pnl),
            Self::A => Ok(inputs.average_realized_pnl),
            Self::W => Ok(inputs.profitable_markets),
            Self::N => Ok(inputs.entered_markets),
            Self::Add(left, right) => left.evaluate(inputs)?.add(right.evaluate(inputs)?),
            Self::Subtract(left, right) => left.evaluate(inputs)?.subtract(right.evaluate(inputs)?),
            Self::Multiply(left, right) => left.evaluate(inputs)?.multiply(right.evaluate(inputs)?),
            Self::Divide(left, right) => left.evaluate(inputs)?.divide(right.evaluate(inputs)?),
            Self::Max(left, right) => {
                let left = left.evaluate(inputs)?;
                let right = right.evaluate(inputs)?;
                if left.compare(right)?.is_lt() {
                    Ok(right)
                } else {
                    Ok(left)
                }
            }
        }
    }
}

struct Parser<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Parser<'a> {
    fn new(expression: &'a str) -> Self {
        Self {
            bytes: expression.as_bytes(),
            offset: 0,
        }
    }

    fn parse(mut self) -> Result<Expression> {
        let expression = self.addition()?;
        self.skip_space();
        ensure!(
            self.offset == self.bytes.len(),
            "unexpected objective expression token at byte {}",
            self.offset
        );
        Ok(expression)
    }

    fn addition(&mut self) -> Result<Expression> {
        let mut left = self.multiplication()?;
        loop {
            if self.consume(b'+') {
                left = Expression::Add(Box::new(left), Box::new(self.multiplication()?));
            } else if self.consume(b'-') {
                left = Expression::Subtract(Box::new(left), Box::new(self.multiplication()?));
            } else {
                return Ok(left);
            }
        }
    }

    fn multiplication(&mut self) -> Result<Expression> {
        let mut left = self.primary()?;
        loop {
            if self.consume(b'*') {
                left = Expression::Multiply(Box::new(left), Box::new(self.primary()?));
            } else if self.consume(b'/') {
                left = Expression::Divide(Box::new(left), Box::new(self.primary()?));
            } else {
                return Ok(left);
            }
        }
    }

    fn primary(&mut self) -> Result<Expression> {
        self.skip_space();
        if self.consume(b'(') {
            let expression = self.addition()?;
            self.expect(b')')?;
            return Ok(expression);
        }
        if self.consume(b'-') {
            return Ok(Expression::Subtract(
                Box::new(Expression::Number(0)),
                Box::new(self.primary()?),
            ));
        }
        let start = self.offset;
        while self.bytes.get(self.offset).is_some_and(u8::is_ascii_digit) {
            self.offset += 1;
        }
        if self.offset > start {
            let digits = std::str::from_utf8(&self.bytes[start..self.offset])?;
            return Ok(Expression::Number(
                digits
                    .parse()
                    .context("objective expression integer overflow")?,
            ));
        }
        while self
            .bytes
            .get(self.offset)
            .is_some_and(u8::is_ascii_alphabetic)
        {
            self.offset += 1;
        }
        let identifier = std::str::from_utf8(&self.bytes[start..self.offset])?;
        match identifier {
            "P" => Ok(Expression::P),
            "A" => Ok(Expression::A),
            "W" => Ok(Expression::W),
            "N" => Ok(Expression::N),
            "max" => {
                self.expect(b'(')?;
                let left = self.addition()?;
                self.expect(b',')?;
                let right = self.addition()?;
                self.expect(b')')?;
                Ok(Expression::Max(Box::new(left), Box::new(right)))
            }
            _ => bail!("unknown objective expression token at byte {}", start),
        }
    }

    fn skip_space(&mut self) {
        while self
            .bytes
            .get(self.offset)
            .is_some_and(u8::is_ascii_whitespace)
        {
            self.offset += 1;
        }
    }

    fn consume(&mut self, byte: u8) -> bool {
        self.skip_space();
        if self.bytes.get(self.offset) == Some(&byte) {
            self.offset += 1;
            true
        } else {
            false
        }
    }

    fn expect(&mut self, byte: u8) -> Result<()> {
        ensure!(
            self.consume(byte),
            "expected '{}' in objective expression at byte {}",
            char::from(byte),
            self.offset
        );
        Ok(())
    }
}
