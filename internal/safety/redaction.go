package safety

import "net/url"

func RedactDSN(dsn string) string {
    u, err := url.Parse(dsn)
    if err != nil {
        return dsn
    }
    if u.User != nil {
        if _, hasPwd := u.User.Password(); hasPwd {
            u.User = url.UserPassword(u.User.Username(), "***")
        }
    }
    return u.String()
}
