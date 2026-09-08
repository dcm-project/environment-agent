package auth

import "context"

type claimsKey struct{}

// WithClaims stores JWT claims in the context.
func WithClaims(ctx context.Context, claims *JWTClaims) context.Context {
	return context.WithValue(ctx, claimsKey{}, claims)
}

// ClaimsFromContext retrieves JWT claims stored by the auth middleware.
// The bool distinguishes "no auth ran" from "auth ran with empty claims".
func ClaimsFromContext(ctx context.Context) (*JWTClaims, bool) {
	claims, ok := ctx.Value(claimsKey{}).(*JWTClaims)
	return claims, ok
}
