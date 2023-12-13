#' @title Exposure Calculation
#' 
#' @description To calculate exposure number,exposure amount,actual number,actual amount,lapse number,lapse amount and corresponding value based on ibnr factors. `r lifecycle::badge('experimental')`
#'
#' @param dt Dataset based on unique policy_id and benefit_id
#' @param max_cnt Maximum number of counts for report_yr and Re_SAR
#' @param is_claim If true, the data consists of mapped claim dataset
#' @param is_ibnr If true, the data consists of ibnr factors
#' @param lapse_rate if setup_lapse_rate is run then true.
#' @param aggregate If true,based on class_var and num_var the values are
#'   aggregated. If false,policy level information can be obtained
#' @param class_var If aggregate=T,a vector of class variables is needed to
#'   obtain values in aggregate level
#' @param num_var If aggregate=T,a vector of numeric variables is needed to
#'   obtain values in aggregate level
#'
#' @return The function returns the calculated exposure values for the datasets
#' @import data.table
#'
#' @examples
#' \dontrun{
#' exposure_cal(if_clm_all_ibnr, 3)
#'
#' # If using new lapse methodology
#' exposure_cal(if_clm_ibnr, 3, lapse_rate = T)
#' }
#' @export
exposure_cal <- function(dt, max_cnt, is_claim = T, is_ibnr = T, lapse_rate = F, aggregate = F, class_var = NA, num_var = NA) {
  if (is_claim) {
    need_cols <- c(
      "Policy_Id", "Benefit_Id", "Policy_Issue_Date", "Insured_BirthDate",
      "Benefit_ClaimDate", "Payment_ReinClaimAmount", "Expo_End_Date"
    )
  } else {
    need_cols <- c(
      "Policy_Id", "Benefit_Id", "Policy_Issue_Date", "Insured_BirthDate",
      "Expo_End_Date"
    )
  }

  new_cols <- names(dt)
  if (sum(is.na(match(need_cols, new_cols))) > 0) {
    cat(
      "Columns Missing/not matching:", need_cols[is.na(match(need_cols, new_cols))], "\n",
      "Following columns are compulsorily needed:", need_cols, "\n"
    )
    stop("Column Names not matching or fields missing. Please make necessary changes")
  } else {
    if (ageDef == "ageLast") {
      dt[
        !is.na(Insured_BirthDate) & !is.na(Policy_Issue_Date),
        iss_age := as.integer((Policy_Issue_Date - Insured_BirthDate) / 365.25)
      ]
    }

    if (ageDef == "ageNearest") {
      dt[
        !is.na(Insured_BirthDate) & !is.na(Policy_Issue_Date),
        iss_age := as.integer(((Policy_Issue_Date - Insured_BirthDate) / 365.25) + 0.5)
      ]
    }

    if (ageDef == "ageNext") {
      dt[
        !is.na(Insured_BirthDate) & !is.na(Policy_Issue_Date),
        iss_age := as.integer((Policy_Issue_Date - Insured_BirthDate) / 365.25) + 1
      ]
    }

    calyear <- data.table(calyear = year(obsstart):year(obsend))
    data <- dt.crossjoin(dt, calyear)

    data[, ":="(issmo = month(Policy_Issue_Date),
      issdy = day(Policy_Issue_Date),
      issyr = year(Policy_Issue_Date))]

    if (lapse_rate) {
      data[, ":="(start = pmax(Policy_Issue_Date, obsstart, na.rm = T),
        end = pmin(Expo_End_Date_Lps, obsend, na.rm = T))] %>%
        .[, ":="(startyr = year(start), endyr = year(end))]
    } else {
      data[, ":="(start = pmax(Policy_Issue_Date, obsstart, na.rm = T),
        end = pmin(Expo_End_Date, obsend, na.rm = T))] %>%
        .[, ":="(startyr = year(start), endyr = year(end))]
    }


    # made change here...from issyr to startyr
    data <- data[calyear >= startyr & calyear <= endyr, ]

    data[, anniv := fifelse(
      issmo == 2 & issdy == 29 & !leap_year(calyear),
      make_date(calyear, issmo, 28),
      make_date(calyear, issmo, issdy)
    )]

    part <- data.table(part = 1:2)
    data <- dt.crossjoin(data, part)

    data[, dur := fifelse(part == 1, calyear - issyr, calyear - issyr + 1)]

    data[part == 1, ":="(annivyr = calyear - 1,
      age_calc = iss_age + dur - 1,
      Xvldfr = pmax(make_date(calyear, 1, 1),start),  #Change 2
      Xvldto = pmin(anniv, end))]

    data[part == 2, ":="(annivyr = calyear,
      age_calc = iss_age + dur - 1,
      Xvldfr = pmax(anniv + days(1), start),
      Xvldto = pmin(make_date(calyear, 12, 31), end))]

    data <- data[Xvldfr >= start & Xvldto >= Xvldfr & dur > 0, ]  #Change 1

    data[, days := as.integer(Xvldto - Xvldfr + 1)]
    data[, expono := fifelse(leap_year(calyear), days / 366, days / 365)]

    data[, polface := amt1][, tmpyr := fifelse(issmo == 1 & issdy == 1, calyear, annivyr)]

    for (i in 1:max_cnt) {
      amtyr.2 <- paste0("amtyr", i)
      amt.1 <- paste0("amt", i)
      data[get(amtyr.2) == tmpyr, ":="(faceamt = get(amt.1), j = i)]
      if (i > 1) {
        amtyr.1 <- paste0("amtyr", i - 1)
        amt.2 <- paste0("amt", i - 1)
        data[
          get(amtyr.1) < tmpyr & (tmpyr < get(amtyr.2) | is.na(get(amtyr.2))),
          ":="(faceamt = get(amt.2), j = i)
        ]
      }
    }

    data[is.na(faceamt) | faceamt == 0, faceamt := fifelse(polface > 0, polface, 0)]

    data[, expoamt := expono * faceamt]
    data[, ":="(l_expono = expono, l_expoamt = expoamt)]

    if (is_claim) {
      data[, ":="(actno = 0,
        actamt = 0,
        lpsno = 0,
        lpsamt = 0)]

      data[Expo_End_Date > obsend, afterend := "Y"]

      if (is_ibnr) {
        data[, ":="(actno_ibnr = 0,
          actamt_ibnr = 0)]

        data[
          Xvldto == end & is.na(afterend) & Benefit_ClaimDate >= Xvldfr &
            Benefit_ClaimDate <= Xvldto,
          ":="(actno = 1,
            actno_ibnr = acum_ibnr_n,
            actamt = Payment_ReinClaimAmount,
            actamt_ibnr = Payment_ReinClaimAmount * acum_ibnr_a)
        ]
      } else {
        data[
          Xvldto == end & is.na(afterend) & Benefit_ClaimDate >= Xvldfr &
            Benefit_ClaimDate <= Xvldto,
          ":="(actno = 1,
            actamt = Payment_ReinClaimAmount)
        ]
      }

      if (lapse_rate) {
        data[, ":="(l_lpsno = 0,
          l_lpsamt = 0)]
        data1 <- data[Expo_End_Date_Lps != Expo_End_Date, ]
        data1[, cut_dt := Expo_End_Date]
        data1 <- split_exposure(data1,
          split_var = "cut_dt", split_ind_name = "lapse_rate_ind",
          split_ind = c("N", "Y")
        )
        data <- rbind(data[Expo_End_Date_Lps == Expo_End_Date, ], data1, fill = T)
        rm(data1)
        data[is.na(lapse_rate_ind), lapse_rate_ind := "N"]

        data[
          Xvldto == Expo_End_Date & is.na(afterend) & is.na(Benefit_ClaimDate) &
            actno < 1 & Expo_End_Date < obsend & lapse_rate_ind == "N",
          ":="(lpsno = 1,
            lpsamt = faceamt)
        ]

        data[
          Xvldto == Expo_End_Date_Lps & Expo_End_Date_Lps <= obsend &
            is.na(Benefit_ClaimDate) &
            actno < 1 & Expo_End_Date_Lps < obsend,
          ":="(l_lpsno = 1,
            l_lpsamt = faceamt)
        ]
      } else {
        data[
          Xvldto == Expo_End_Date & is.na(afterend) & is.na(Benefit_ClaimDate) &
            actno < 1 & Expo_End_Date < obsend,
          ":="(lpsno = 1,
            lpsamt = faceamt)
        ]
      }
    }

    data[, ":="(j = NULL, polface = NULL, faceamt = NULL, afterend = NULL, part = NULL,
      startyr = NULL, endyr = NULL, annivyr = NULL, tmpyr = NULL,
      issmo = NULL, issdy = NULL)]
    if (aggregate) {
      data <- dt.summarise(data, class_var = class_var, num_var = num_var)
    }

    return(data)
  }
}



#' @title Split Exposure
#' 
#' @description To split the exposure into two parts based on the defined cut date. `r lifecycle::badge('experimental')`
#'
#' @param data exposure dataset
#' @param split_var name of the variable used to split exposure
#' @param split_ind two indicators in vector form denoting what does the split indicate in two parts.For example, a waiting period will indicate 'Y' for before cut date and 'N' for after cut date
#' @param split_ind_name Name of the indicator variable
#' @param is_claim If true, the data consists of mapped claim dataset
#' @param is_ibnr If true, the data consists of ibnr factors
#' @param lapse_rate if setup_lapse_rate is run then true.
#' @param aggregate If true,based on class_var and num_var the values are aggregated. If false,policy level information can be obtained
#' @param class_var If aggregate=T,a vector of class variables is needed to obtain values in aggregate level
#' @param num_var If aggregate=T,a vector of numeric variables is needed to obtain values in aggregate level
#'
#' @return The function returns the exposure dataset split into two parts based on the defined cut date
#' @import data.table
#'
#' @examples
#' \dontrun{
#' exposure_cal_1 <- split_exposure(setdates, split_by = "cut_dt", split_ind_name = "waiting_period_ind", split_ind = c("Y", "N"))
#'
#' # In case the new lapse methodology is run on exposure, split exposure also needs to configured with
#' exposure_cal_1 <- split_exposure(setdates, split_by = "cut_dt", split_ind_name = "waiting_period_ind", split_ind = c("Y", "N"), lapse_rate = T)
#' }
#' @export

split_exposure <- function(data, split_var, split_ind_name, split_ind, is_claim = T, is_ibnr = T, lapse_rate = F, aggregate = F, class_var = NA, num_var = NA) {
  if (nrow(data) == 0) {
    stop("Empty dataset....No splitting required")
  }
  if (split_var != "cut_dt") {
    setnames(data, split_var, "cut_dt")
  }
  if (sum(is.na(data$cut_dt)) > 0) {
    stop("Missing values in splitting variable")
  } else {
    data[, ":="(Xvldfr_ = Xvldfr,
      Xvldto_ = Xvldto,
      expono_ = expono,
      expoamt_ = expoamt,
      l_expono_ = l_expono,
      l_expoamt_ = l_expoamt,
      days_ = days)]

    #*** period before the cutting point ***
    d1 <- data[cut_dt >= Xvldto, ]
    d2 <- data[cut_dt >= Xvldfr & cut_dt < Xvldto, ]
    d3 <- data[!((cut_dt >= Xvldto) | (cut_dt >= Xvldfr & cut_dt < Xvldto)), ]

    data[, ":="(Xvldfr_ = NULL,
      Xvldto_ = NULL,
      expono_ = NULL,
      expoamt_ = NULL,
      l_expono_ = NULL,
      l_expoamt_ = NULL,
      days_ = NULL)]


    d3[, before_cut_dt := 0]
    if (nrow(d2) > 0) {
      d1[, before_cut_dt := 1]
      dt_cut <- data.table(before_cut_dt = 0:1)
      d2 <- dt.crossjoin(d2, dt_cut)
      d2[before_cut_dt == 0, ":="(
        Xvldfr = cut_dt + days(1),
        days = as.integer(Xvldto - (cut_dt + days(1))) + 1)]

      d2[before_cut_dt == 0, ":="(
        expono = expono_ * days / days_,
        expoamt = expoamt_ * days / days_,
        l_expono = l_expono_ * days / days_,
        l_expoamt = l_expoamt_ * days / days_)]

      d2[before_cut_dt == 1, ":="(
        Xvldfr = Xvldfr_,
        Xvldto = cut_dt,
        days = as.integer(cut_dt - Xvldfr_) + 1)]

      if (is_claim) {
        d2[before_cut_dt == 1, ":="(
          actno = 0,
          actamt = 0,
          lpsno = 0,
          lpsamt = 0)]

        if (lapse_rate) {
          d2[before_cut_dt == 1, ":="(
            l_lpsno = 0,
            l_lpsamt = 0)]
        }

        if (is_ibnr) {
          d2[before_cut_dt == 1, ":="(
            actno_ibnr = 0,
            actamt_ibnr = 0)]
        }
      }

      d2[before_cut_dt == 1, ":="(
        expono = expono_ * days / days_,
        expoamt = expoamt_ * days / days_,
        l_expono = l_expono_ * days / days_,
        l_expoamt = l_expoamt_ * days / days_)]

      data <- rbind(d1, d2, d3)
    } else {
      if (nrow(d1) > 0) {
        d1[, before_cut_dt := 1]
        data <- rbind(d1, d3)
      } else {
        data <- d3
        print("No observations containing cut date")
      }
    }

    data[, ":="(Xvldfr_ = NULL,
      Xvldto_ = NULL,
      expono_ = NULL,
      expoamt_ = NULL,
      l_expono_ = NULL,
      l_expoamt_ = NULL,
      days_ = NULL,
      cut_dt = NULL)]

    data[, (split_ind_name) := fifelse(before_cut_dt == 1, split_ind[1], split_ind[2])]
    data[, before_cut_dt := NULL]

    if (aggregate) {
      data <- dt.summarise(data, class_var = class_var, num_var = num_var)
    }
    return(data)
  }
}
